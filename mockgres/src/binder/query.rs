use super::*;

pub(super) fn bind_with_search_path(
    db: &Db,
    search_path: &[SchemaId],
    current_database: Option<&str>,
    time_ctx: BindTimeContext,
    cte_scope: &CteScope,
    p: Plan,
) -> PgWireResult<Plan> {
    match p {
        Plan::Empty => Ok(Plan::Empty),
        Plan::DeclareCursor { .. } | Plan::FetchCursor { .. } => {
            unreachable!("cursor plans are handled by bind()")
        }
        Plan::Values { rows, schema } => {
            let empty_schema = Schema { fields: vec![] };
            let mut bound_rows = Vec::with_capacity(rows.len());
            for row in rows {
                let mut bound_row = Vec::with_capacity(row.len());
                for (idx, expr) in row.into_iter().enumerate() {
                    let hint = schema.fields.get(idx).map(|field| &field.data_type);
                    let bound = match expr {
                        Expr::Literal(value) => Expr::Literal(value),
                        Expr::Column(idx) => Expr::Column(idx),
                        Expr::Scalar(expr) => Expr::Scalar(bind_scalar_expr(
                            &expr,
                            &empty_schema,
                            hint,
                            db,
                            search_path,
                            current_database,
                            time_ctx,
                        )?),
                    };
                    bound_row.push(bound);
                }
                bound_rows.push(bound_row);
            }
            Ok(Plan::Values {
                rows: bound_rows,
                schema,
            })
        }
        Plan::With { ctes, body } => {
            let mut scoped = cte_scope.clone();
            let mut pending = ctes;
            let mut bound_ctes = Vec::with_capacity(pending.len());
            while !pending.is_empty() {
                let unresolved_names: HashSet<String> =
                    pending.iter().map(|cte| cte.name.clone()).collect();
                let mut next_pending = Vec::new();
                let mut made_progress = false;
                for mut cte in pending {
                    match bind_with_search_path(
                        db,
                        search_path,
                        current_database,
                        time_ctx,
                        &scoped,
                        *cte.plan.clone(),
                    ) {
                        Ok(bound_plan) => {
                            let mut output_schema = bound_plan.schema().clone();
                            if let Some(output_columns) = &cte.output_columns {
                                if output_columns.len() != output_schema.fields.len() {
                                    return Err(fe_code(
                                        "42601",
                                        format!(
                                            "CTE \"{}\" has {} columns but {} column aliases were provided",
                                            cte.name,
                                            output_schema.fields.len(),
                                            output_columns.len()
                                        ),
                                    ));
                                }
                                for (field, alias) in
                                    output_schema.fields.iter_mut().zip(output_columns)
                                {
                                    field.name = alias.clone();
                                }
                            }
                            cte.plan = Box::new(bound_plan);
                            cte.schema = Some(output_schema.clone());
                            scoped.insert(
                                cte.name.clone(),
                                CteBinding {
                                    schema: output_schema,
                                },
                            );
                            bound_ctes.push(cte);
                            made_progress = true;
                        }
                        Err(err)
                            if should_defer_cte_binding(&err, &unresolved_names, &cte.name) =>
                        {
                            next_pending.push(cte);
                        }
                        Err(err) => return Err(err),
                    }
                }
                if !made_progress {
                    return Err(fe_code(
                        "0A000",
                        "circular CTE dependencies are not supported",
                    ));
                }
                pending = next_pending;
            }
            let bound_body =
                bind_with_search_path(db, search_path, current_database, time_ctx, &scoped, *body)?;
            Ok(Plan::With {
                ctes: bound_ctes,
                body: Box::new(bound_body),
            })
        }
        Plan::UnboundSeqScan {
            mut table,
            alias,
            selection,
            lock,
        } => {
            if table.name == "pg_shmem_allocations_numa" {
                return Err(fe(
                    "libnuma initialization failed or NUMA is not supported on this platform",
                ));
            }
            if table.schema.is_none()
                && let Some(cte) = cte_scope.get(&table.name)
            {
                let cte_schema = &cte.schema;
                let base_origin = Some(FieldOrigin {
                    schema: None,
                    table: Some(table.name.clone()),
                    alias: alias.clone(),
                });
                let cols: Vec<(usize, Field)> = match selection {
                    Selection::Star => cte_schema
                        .fields
                        .iter()
                        .enumerate()
                        .map(|(i, c)| {
                            (
                                i,
                                Field {
                                    name: c.name.clone(),
                                    data_type: c.data_type.clone(),
                                    origin: base_origin.clone(),
                                },
                            )
                        })
                        .collect(),
                    Selection::Columns(names) => {
                        let mut out = Vec::with_capacity(names.len());
                        for n in names {
                            let i = cte_schema
                                .fields
                                .iter()
                                .position(|c| c.name == n)
                                .ok_or_else(|| fe_code("42703", format!("unknown column: {n}")))?;
                            if out.iter().any(|(existing_idx, _)| *existing_idx == i) {
                                continue;
                            }
                            out.push((
                                i,
                                Field {
                                    name: n,
                                    data_type: cte_schema.fields[i].data_type.clone(),
                                    origin: base_origin.clone(),
                                },
                            ));
                        }
                        out
                    }
                };
                let schema = Schema {
                    fields: cols.iter().map(|(_, f)| f.clone()).collect(),
                };
                return Ok(Plan::CteScan {
                    name: table.name,
                    cols,
                    schema,
                });
            }
            let tm = resolve_table_meta(db, search_path, &table).map_err(map_catalog_err)?;
            if table.schema.is_none() {
                table.schema = Some(tm.schema.clone());
            }

            let schema_name = tm.schema.as_str().to_string();
            let table_name = tm.name.clone();
            let alias_clone = alias.clone();
            let base_origin = Some(FieldOrigin {
                schema: Some(schema_name),
                table: Some(table_name),
                alias: alias_clone,
            });

            // build (idx, Field) for executor + compose output schema
            let cols: Vec<(usize, Field)> = match selection {
                Selection::Star => tm
                    .columns
                    .iter()
                    .enumerate()
                    .map(|(i, c)| {
                        (
                            i,
                            Field {
                                name: c.name.clone(),
                                data_type: c.data_type.clone(),
                                origin: base_origin.clone(),
                            },
                        )
                    })
                    .collect(),
                Selection::Columns(names) => {
                    let mut out = Vec::with_capacity(names.len());
                    for n in names {
                        let i = tm
                            .columns
                            .iter()
                            .position(|c| c.name == n)
                            .ok_or_else(|| fe_code("42703", format!("unknown column: {n}")))?;
                        if out.iter().any(|(existing_idx, _)| *existing_idx == i) {
                            continue;
                        }
                        out.push((
                            i,
                            Field {
                                name: n,
                                data_type: tm.columns[i].data_type.clone(),
                                origin: base_origin.clone(),
                            },
                        ));
                    }
                    out
                }
            };
            let mut schema = Schema {
                fields: cols.iter().map(|(_, f)| f.clone()).collect(),
            };
            if lock.is_some() {
                schema.fields.push(Field {
                    name: "__mockgres_row_id".to_string(),
                    data_type: DataType::Int8,
                    origin: None,
                });
            }
            let lock = lock.map(|req| LockSpec {
                mode: req.mode,
                skip_locked: req.skip_locked,
                nowait: req.nowait,
                target: tm.id,
            });

            Ok(Plan::SeqScan {
                table,
                cols,
                schema,
                lock,
            })
        }
        Plan::Alias {
            input,
            alias,
            schema: _,
        } => {
            let child = bind_with_search_path(
                db,
                search_path,
                current_database,
                time_ctx,
                cte_scope,
                *input,
            )?;
            let mut fields = Vec::with_capacity(child.schema().fields.len());
            let mut exprs = Vec::with_capacity(child.schema().fields.len());
            for (idx, f) in child.schema().fields.iter().enumerate() {
                let name = alias.column_names.get(idx).unwrap_or(&f.name);
                fields.push(Field {
                    name: name.clone(),
                    data_type: f.data_type.clone(),
                    origin: Some(FieldOrigin {
                        schema: None,
                        table: None,
                        alias: Some(alias.alias.clone()),
                    }),
                });
                exprs.push((ScalarExpr::ColumnIdx(idx), name.clone()));
            }
            Ok(Plan::Projection {
                input: Box::new(child),
                exprs,
                schema: Schema { fields },
            })
        }
        Plan::UnboundJoin {
            left,
            right,
            join_type,
            on,
        } => {
            let left_bound = bind_with_search_path(
                db,
                search_path,
                current_database,
                time_ctx,
                cte_scope,
                *left,
            )?;
            let right_bound = bind_with_search_path(
                db,
                search_path,
                current_database,
                time_ctx,
                cte_scope,
                *right,
            )?;
            let mut fields = left_bound.schema().fields.clone();
            fields.extend(right_bound.schema().fields.clone());
            let schema = Schema { fields };
            let bound_on = if let Some(expr) = on.as_ref() {
                Some(bind_bool_expr(
                    expr,
                    &schema,
                    db,
                    search_path,
                    current_database,
                    time_ctx,
                    cte_scope,
                )?)
            } else {
                None
            };
            Ok(Plan::Join {
                left: Box::new(left_bound),
                right: Box::new(right_bound),
                on: bound_on,
                join_type,
                schema,
            })
        }

        Plan::Projection {
            input,
            exprs,
            schema: _,
        } => {
            let mut child = bind_with_search_path(
                db,
                search_path,
                current_database,
                time_ctx,
                cte_scope,
                *input,
            )?;
            let child_schema = child.schema().clone();
            let mut window_specs = Vec::new();
            let mut window_expr_indexes = Vec::new();
            for (idx, (expr, name)) in exprs.iter().enumerate() {
                if let ScalarExpr::WindowRowNumber(spec) = expr {
                    window_specs.push((
                        bind_window_spec(
                            spec,
                            &child_schema,
                            db,
                            search_path,
                            current_database,
                            time_ctx,
                        )?,
                        name.clone(),
                    ));
                    window_expr_indexes
                        .push((idx, child_schema.fields.len() + window_specs.len() - 1));
                }
            }
            if !window_specs.is_empty() {
                let mut fields = child_schema.fields.clone();
                for (_, name) in &window_specs {
                    fields.push(Field {
                        name: name.clone(),
                        data_type: DataType::Int8,
                        origin: None,
                    });
                }
                child = Plan::WindowRowNumber {
                    input: Box::new(child),
                    specs: window_specs,
                    schema: Schema { fields },
                };
            }
            let mut bound_exprs = Vec::with_capacity(exprs.len());
            let mut fields = Vec::with_capacity(exprs.len());
            for (idx, (expr, name)) in exprs.into_iter().enumerate() {
                if matches!(
                    &expr,
                    ScalarExpr::Column(column)
                        if column.schema.is_none()
                            && column.relation.is_none()
                            && column.column == "*"
                ) {
                    for (column_index, field) in child.schema().fields.iter().enumerate() {
                        fields.push(field.clone());
                        bound_exprs.push((ScalarExpr::ColumnIdx(column_index), field.name.clone()));
                    }
                    continue;
                }
                let bound = if let Some((_, col_idx)) = window_expr_indexes
                    .iter()
                    .find(|(expr_idx, _)| *expr_idx == idx)
                {
                    ScalarExpr::ColumnIdx(*col_idx)
                } else {
                    bind_scalar_expr(
                        &expr,
                        child.schema(),
                        None,
                        db,
                        search_path,
                        current_database,
                        time_ctx,
                    )?
                };
                let dt = scalar_expr_type(&bound, child.schema()).unwrap_or(DataType::Text);
                fields.push(Field {
                    name: name.clone(),
                    data_type: dt,
                    origin: None,
                });
                bound_exprs.push((bound, name.clone()));
            }
            Ok(Plan::Projection {
                input: Box::new(child),
                exprs: bound_exprs,
                schema: Schema { fields },
            })
        }

        Plan::WindowRowNumber { .. } => Err(fe("window plan cannot be bound directly")),

        Plan::Aggregate {
            input,
            group_exprs,
            agg_exprs,
            schema: _,
        } => {
            let child = bind_with_search_path(
                db,
                search_path,
                current_database,
                time_ctx,
                cte_scope,
                *input,
            )?;
            let child_schema = child.schema().clone();
            let mut bound_group_exprs = Vec::with_capacity(group_exprs.len());
            let mut fields = Vec::new();
            for (expr, name) in group_exprs {
                let bound = bind_scalar_expr(
                    &expr,
                    &child_schema,
                    None,
                    db,
                    search_path,
                    current_database,
                    time_ctx,
                )?;
                let dt = scalar_expr_type(&bound, &child_schema).unwrap_or(DataType::Text);
                let origin = if let ScalarExpr::ColumnIdx(idx) = &bound {
                    child_schema.fields.get(*idx).and_then(|f| f.origin.clone())
                } else {
                    None
                };
                fields.push(Field {
                    name: name.clone(),
                    data_type: dt.clone(),
                    origin,
                });
                bound_group_exprs.push((bound, name));
            }

            let mut bound_agg_exprs = Vec::with_capacity(agg_exprs.len());
            for (agg_call, name) in agg_exprs {
                let bound_expr = if let Some(expr) = agg_call.expr {
                    Some(bind_scalar_expr(
                        &expr,
                        &child_schema,
                        None,
                        db,
                        search_path,
                        current_database,
                        time_ctx,
                    )?)
                } else {
                    None
                };
                let dt = match agg_call.func {
                    AggFunc::Count => DataType::Int8,
                    AggFunc::Sum => match bound_expr
                        .as_ref()
                        .and_then(|e| scalar_expr_type(e, &child_schema))
                    {
                        Some(DataType::Float8) => DataType::Float8,
                        Some(DataType::Int4) | Some(DataType::Int8) => DataType::Int8,
                        _ => DataType::Float8,
                    },
                    AggFunc::Avg => DataType::Float8,
                    AggFunc::Min | AggFunc::Max => bound_expr
                        .as_ref()
                        .and_then(|e| scalar_expr_type(e, &child_schema))
                        .unwrap_or(DataType::Text),
                    AggFunc::BoolAnd => DataType::Bool,
                };
                fields.push(Field {
                    name: name.clone(),
                    data_type: dt.clone(),
                    origin: None,
                });
                bound_agg_exprs.push((
                    AggCall {
                        func: agg_call.func,
                        expr: bound_expr,
                        distinct: agg_call.distinct,
                    },
                    name,
                ));
            }

            Ok(Plan::Aggregate {
                input: Box::new(child),
                group_exprs: bound_group_exprs,
                agg_exprs: bound_agg_exprs,
                schema: Schema { fields },
            })
        }

        Plan::CountRows { input, schema } => {
            let child = bind_with_search_path(
                db,
                search_path,
                current_database,
                time_ctx,
                cte_scope,
                *input,
            )?;
            Ok(Plan::CountRows {
                input: Box::new(child),
                schema,
            })
        }
        Plan::LockRows {
            mut table,
            input,
            lock,
            row_id_idx: _,
            schema: _,
        } => {
            let bound_child = bind_with_search_path(
                db,
                search_path,
                current_database,
                time_ctx,
                cte_scope,
                *input,
            )?;
            if matches!(bound_child, Plan::CteScan { .. }) {
                return Ok(bound_child);
            }
            let child_schema = bound_child.schema().clone();
            if child_schema.fields.is_empty() {
                return Err(fe("FOR UPDATE requires row identifier column"));
            }
            let row_id_idx = child_schema.fields.len() - 1;
            let mut visible_fields = child_schema.fields.clone();
            visible_fields.remove(row_id_idx);
            let schema = Schema {
                fields: visible_fields,
            };
            let tm = resolve_table_meta(db, search_path, &table).map_err(map_catalog_err)?;
            if table.schema.is_none() {
                table.schema = Some(tm.schema.clone());
            }
            Ok(Plan::LockRows {
                table,
                input: Box::new(bound_child),
                lock: LockSpec {
                    mode: lock.mode,
                    skip_locked: lock.skip_locked,
                    nowait: lock.nowait,
                    target: tm.id,
                },
                row_id_idx,
                schema,
            })
        }

        // wrappers: bind child; nothing else to do
        Plan::Filter {
            input,
            expr,
            project_prefix_len,
        } => {
            let child = bind_with_search_path(
                db,
                search_path,
                current_database,
                time_ctx,
                cte_scope,
                *input,
            )?;
            let bound_expr = bind_bool_expr(
                &expr,
                child.schema(),
                db,
                search_path,
                current_database,
                time_ctx,
                cte_scope,
            )?;
            let mut plan = match child {
                Plan::Join {
                    left,
                    right,
                    on,
                    join_type: JoinType::Inner,
                    schema,
                } => {
                    let on = Some(match on {
                        Some(existing) => BoolExpr::And(vec![existing, bound_expr]),
                        None => bound_expr,
                    });
                    Plan::Join {
                        left,
                        right,
                        on,
                        join_type: JoinType::Inner,
                        schema,
                    }
                }
                child => Plan::Filter {
                    input: Box::new(child),
                    expr: bound_expr,
                    project_prefix_len: None,
                },
            };
            if let Some(n) = project_prefix_len {
                if n == 0 {
                    return Ok(plan);
                }
                let schema = plan.schema().clone();
                let fields = schema.fields[..n].to_vec();
                let proj_exprs = (0..n)
                    .map(|i| (ScalarExpr::ColumnIdx(i), fields[i].name.clone()))
                    .collect();
                plan = Plan::Projection {
                    input: Box::new(plan),
                    exprs: proj_exprs,
                    schema: Schema { fields },
                };
            }
            Ok(plan)
        }
        Plan::Update {
            mut table,
            table_alias,
            sets,
            filter,
            from,
            mut from_schema,
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
                alias: table_alias.clone(),
            });
            let target_schema = Schema {
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
            let (bound_from, combined_schema) = if let Some(plan) = from {
                let bound = bind_with_search_path(
                    db,
                    search_path,
                    current_database,
                    time_ctx,
                    cte_scope,
                    *plan,
                )?;
                let mut fields = target_schema.fields.clone();
                fields.extend(bound.schema().fields.clone());
                from_schema = Some(bound.schema().clone());
                (Some(bound), Schema { fields })
            } else {
                (None, target_schema.clone())
            };
            let bound_sets = bind_update_sets(
                sets,
                &combined_schema,
                tm,
                db,
                search_path,
                current_database,
                time_ctx,
                false,
            )?;
            let bound_filter = match filter {
                Some(f) => Some(bind_bool_expr(
                    &f,
                    &combined_schema,
                    db,
                    search_path,
                    current_database,
                    time_ctx,
                    cte_scope,
                )?),
                None => None,
            };
            let returning_schema = match returning.as_mut() {
                Some(clause) => Some(bind_returning_clause(
                    clause,
                    &combined_schema,
                    db,
                    search_path,
                    current_database,
                    time_ctx,
                )?),
                None => None,
            };
            Ok(Plan::Update {
                table,
                table_alias,
                sets: bound_sets,
                filter: bound_filter,
                from: bound_from.map(Box::new),
                from_schema,
                returning,
                returning_schema,
            })
        }
        Plan::Delete {
            mut table,
            table_alias,
            filter,
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
                alias: table_alias.clone(),
            });
            let schema = Schema {
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
            let bound_filter = match filter {
                Some(f) => Some(bind_bool_expr(
                    &f,
                    &schema,
                    db,
                    search_path,
                    current_database,
                    time_ctx,
                    cte_scope,
                )?),
                None => None,
            };
            let returning_schema = match returning.as_mut() {
                Some(clause) => Some(bind_returning_clause(
                    clause,
                    &schema,
                    db,
                    search_path,
                    current_database,
                    time_ctx,
                )?),
                None => None,
            };
            Ok(Plan::Delete {
                table,
                table_alias,
                filter: bound_filter,
                returning,
                returning_schema,
            })
        }
        Plan::Order { input, keys } => {
            let child = bind_with_search_path(
                db,
                search_path,
                current_database,
                time_ctx,
                cte_scope,
                *input,
            )?;
            let child_schema = child.schema().clone();
            let mut bound_keys = Vec::with_capacity(keys.len());
            for key in keys {
                match key {
                    SortKey::ByName {
                        col,
                        asc,
                        nulls_first,
                    } => {
                        let idx = child_schema
                            .fields
                            .iter()
                            .position(|f| f.name == col)
                            .ok_or_else(|| fe_code("42703", format!("unknown column: {col}")))?;
                        bound_keys.push(SortKey::ByIndex {
                            idx,
                            asc,
                            nulls_first,
                        });
                    }
                    SortKey::Expr {
                        expr,
                        asc,
                        nulls_first,
                    } => {
                        let bound = bind_scalar_expr(
                            &expr,
                            &child_schema,
                            None,
                            db,
                            search_path,
                            current_database,
                            time_ctx,
                        )?;
                        bound_keys.push(SortKey::Expr {
                            expr: bound,
                            asc,
                            nulls_first,
                        });
                    }
                    other => bound_keys.push(other),
                }
            }
            Ok(Plan::Order {
                input: Box::new(child),
                keys: bound_keys,
            })
        }
        Plan::Limit {
            input,
            limit,
            offset,
        } => {
            let child = bind_with_search_path(
                db,
                search_path,
                current_database,
                time_ctx,
                cte_scope,
                *input,
            )?;
            Ok(Plan::Limit {
                input: Box::new(child),
                limit,
                offset,
            })
        }
        other => super::write::bind_write_plan(
            db,
            search_path,
            current_database,
            time_ctx,
            cte_scope,
            other,
        ),
    }
}
