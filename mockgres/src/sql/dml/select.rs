use super::*;

pub(crate) fn try_plan_builtin_select(target: &pg_query::Node) -> PgWireResult<Option<Plan>> {
    use pg_query::NodeEnum;

    let tgt = target
        .node
        .as_ref()
        .ok_or_else(|| fe("unexpected target"))?;
    let NodeEnum::ResTarget(rt) = tgt else {
        return Ok(None);
    };

    let expr_node = rt
        .val
        .as_ref()
        .and_then(|n| n.node.as_ref())
        .ok_or_else(|| fe("missing expr"))?;

    let NodeEnum::FuncCall(fc) = expr_node else {
        return Ok(None);
    };

    // Extract function name (last component)
    let func_name = fc
        .funcname
        .iter()
        .filter_map(|n| {
            n.node.as_ref().and_then(|nn| {
                if let NodeEnum::String(s) = nn {
                    Some(s.sval.to_ascii_lowercase())
                } else {
                    None
                }
            })
        })
        .next_back()
        .unwrap_or_default();

    // Only handle our two builtins here
    if func_name != "mockgres_freeze" && func_name != "mockgres_reset" {
        return Ok(None);
    }

    // No args allowed for now
    if !fc.args.is_empty() {
        return Err(fe(format!("{func_name}() takes no arguments")));
    }

    // Result schema: single bool column
    let col_name = if rt.name.is_empty() {
        func_name.clone()
    } else {
        rt.name.clone()
    };

    let schema = Schema {
        fields: vec![Field {
            name: col_name,
            data_type: DataType::Bool,
            origin: None,
        }],
    };

    Ok(Some(Plan::CallBuiltin {
        name: func_name,
        args: Vec::new(),
        schema,
    }))
}

pub(crate) fn plan_literal_select(sel: SelectStmt) -> PgWireResult<Plan> {
    let where_expr = sel
        .where_clause
        .as_ref()
        .and_then(|node| node.node.as_ref())
        .map(parse_bool_expr)
        .transpose()?;
    let tl = sel.target_list;
    if tl.is_empty() {
        return Err(fe("at least one column required"));
    }
    // check for builtin single-target SELECTs
    if where_expr.is_none()
        && tl.len() == 1
        && let Some(plan) = try_plan_builtin_select(&tl[0])?
    {
        return Ok(plan);
    }
    let mut out_exprs = Vec::with_capacity(tl.len());
    for t in tl {
        let tgt = t.node.as_ref().ok_or_else(|| fe("unexpected target"))?;
        let NodeEnum::ResTarget(rt) = tgt else {
            return Err(fe("unexpected target"));
        };
        let expr_node = rt
            .val
            .as_ref()
            .and_then(|n| n.node.as_ref())
            .ok_or_else(|| fe("missing expr"))?;
        let expr = parse_scalar_expr(expr_node)?;
        let name = if rt.name.is_empty() {
            derive_expr_name(&expr)
        } else {
            rt.name.clone()
        };
        out_exprs.push((expr, name));
    }
    let mut input = Plan::Values {
        rows: vec![vec![]],
        schema: Schema { fields: vec![] },
    };
    if let Some(expr) = where_expr {
        input = Plan::Filter {
            input: Box::new(input),
            expr,
            project_prefix_len: None,
        };
    }
    Ok(Plan::Projection {
        input: Box::new(input),
        exprs: out_exprs,
        schema: Schema { fields: vec![] },
    })
}

pub(crate) fn plan_values_select(sel: SelectStmt) -> PgWireResult<Plan> {
    if !sel.target_list.is_empty()
        || !sel.from_clause.is_empty()
        || sel.where_clause.is_some()
        || !sel.group_clause.is_empty()
        || sel.having_clause.is_some()
        || !sel.locking_clause.is_empty()
    {
        return Err(fe("unsupported VALUES query shape"));
    }

    let mut rows = Vec::with_capacity(sel.values_lists.len());
    let mut column_types: Vec<Option<DataType>> = Vec::new();
    let mut width: Option<usize> = None;
    for value_list in sel.values_lists {
        let Some(NodeEnum::List(list)) = value_list.node else {
            return Err(fe("bad VALUES row"));
        };
        if let Some(expected) = width {
            if list.items.len() != expected {
                return Err(fe_code(
                    "42601",
                    "VALUES rows must all have the same number of columns",
                ));
            }
        } else {
            width = Some(list.items.len());
            column_types.resize(list.items.len(), None);
        }

        let mut row = Vec::with_capacity(list.items.len());
        for (idx, cell) in list.items.into_iter().enumerate() {
            let node = cell.node.as_ref().ok_or_else(|| fe("bad VALUES cell"))?;
            let expr = parse_scalar_expr(node)?;
            let ty = infer_values_expr_type(&expr);
            column_types[idx] = merge_values_type(column_types[idx].clone(), ty);
            row.push(Expr::Scalar(expr));
        }
        rows.push(row);
    }

    let fields = column_types
        .into_iter()
        .enumerate()
        .map(|(idx, ty)| Field {
            name: format!("column{}", idx + 1),
            data_type: ty.unwrap_or(DataType::Text),
            origin: None,
        })
        .collect();
    let mut plan = Plan::Values {
        rows,
        schema: Schema { fields },
    };

    if !sel.sort_clause.is_empty() {
        plan = Plan::Order {
            input: Box::new(plan),
            keys: parse_order_clause(&sel.sort_clause)?,
        };
    }

    let mut limit_value = None;
    if let Some(limit_node) = sel.limit_count.as_ref().and_then(|n| n.node.as_ref()) {
        limit_value = Some(parse_limit_count(limit_node)?);
    }
    let mut offset_value = CountExpr::Value(0);
    if let Some(offset_node) = sel.limit_offset.as_ref().and_then(|n| n.node.as_ref()) {
        offset_value = parse_offset_count(offset_node)?;
    }
    if limit_value.is_some() || !matches!(offset_value, CountExpr::Value(0)) {
        plan = Plan::Limit {
            input: Box::new(plan),
            limit: limit_value,
            offset: offset_value,
        };
    }

    Ok(plan)
}

pub(crate) fn infer_values_expr_type(expr: &ScalarExpr) -> Option<DataType> {
    match expr {
        ScalarExpr::Literal(Value::Int64(i)) => {
            if *i >= i32::MIN as i64 && *i <= i32::MAX as i64 {
                Some(DataType::Int4)
            } else {
                Some(DataType::Int8)
            }
        }
        ScalarExpr::Literal(Value::Float64Bits(_)) => Some(DataType::Float8),
        ScalarExpr::Literal(Value::Text(_)) => Some(DataType::Text),
        ScalarExpr::Literal(Value::Bool(_)) => Some(DataType::Bool),
        ScalarExpr::Literal(Value::Date(_)) => Some(DataType::Date),
        ScalarExpr::Literal(Value::TimestampMicros(_)) => Some(DataType::Timestamp),
        ScalarExpr::Literal(Value::TimestamptzMicros(_)) => Some(DataType::Timestamptz),
        ScalarExpr::Literal(Value::Bytes(_)) => Some(DataType::Bytea),
        ScalarExpr::Literal(Value::IntervalMicros(_)) => Some(DataType::Interval),
        ScalarExpr::Literal(Value::Null) => None,
        ScalarExpr::Cast { ty, .. } => Some(ty.clone()),
        ScalarExpr::Param { ty, .. } => ty.clone(),
        ScalarExpr::Func {
            func: crate::engine::ScalarFunc::Coalesce,
            args,
        } => args.iter().find_map(infer_values_expr_type),
        ScalarExpr::Predicate(_) => Some(DataType::Bool),
        _ => Some(infer_expr_type(expr)),
    }
}

pub(crate) fn merge_values_type(
    existing: Option<DataType>,
    incoming: Option<DataType>,
) -> Option<DataType> {
    match (existing, incoming) {
        (None, next) => next,
        (current, None) => current,
        (Some(DataType::Float8), Some(_)) | (Some(_), Some(DataType::Float8)) => {
            Some(DataType::Float8)
        }
        (Some(DataType::Int8), Some(DataType::Int4))
        | (Some(DataType::Int4), Some(DataType::Int8)) => Some(DataType::Int8),
        (Some(current), Some(incoming)) if current == incoming => Some(current),
        (Some(current), Some(incoming))
            if matches!(
                (&current, &incoming),
                (DataType::Int4, DataType::Int4)
                    | (DataType::Int4, DataType::Int8)
                    | (DataType::Int8, DataType::Int4)
                    | (DataType::Int8, DataType::Int8)
            ) =>
        {
            Some(DataType::Int8)
        }
        _ => Some(DataType::Text),
    }
}

pub(crate) fn parse_select_list(
    target_list: &mut Vec<pg_query::Node>,
) -> PgWireResult<ParsedSelectList> {
    if let Some(t) = target_list.first() {
        let node = t.node.as_ref().ok_or_else(|| fe("missing target node"))?;
        if let NodeEnum::ResTarget(rt) = node
            && let Some(NodeEnum::ColumnRef(cr)) = rt.val.as_ref().and_then(|n| n.node.as_ref())
            && cr
                .fields
                .iter()
                .any(|field| matches!(field.node.as_ref(), Some(NodeEnum::AStar(_))))
        {
            if target_list.len() == 1 {
                return Ok((Selection::Star, None));
            }
            let mut exprs = vec![(
                ScalarExpr::Column(crate::engine::ColumnRefName {
                    schema: None,
                    relation: None,
                    column: "*".to_string(),
                    location: None,
                }),
                "*".to_string(),
            )];
            for target in target_list.drain(1..) {
                let Some(NodeEnum::ResTarget(target)) = target.node.as_ref() else {
                    return Err(fe("bad target"));
                };
                let expression = target
                    .val
                    .as_ref()
                    .and_then(|node| node.node.as_ref())
                    .ok_or_else(|| fe("bad target expr"))?;
                let expression = parse_scalar_expr(expression)?;
                let name = if target.name.is_empty() {
                    derive_expr_name(&expression)
                } else {
                    target.name.clone()
                };
                exprs.push((expression, name));
            }
            target_list.clear();
            return Ok((Selection::Star, Some(exprs)));
        }
    }

    let mut cols = Vec::new();
    let mut exprs = Vec::new();
    for t in target_list.drain(..) {
        let rt = t
            .node
            .as_ref()
            .and_then(|n| {
                if let NodeEnum::ResTarget(rt) = n {
                    Some(rt)
                } else {
                    None
                }
            })
            .ok_or_else(|| fe("bad target"))?;
        let expr_node = rt
            .val
            .as_ref()
            .and_then(|n| n.node.as_ref())
            .ok_or_else(|| fe("bad target expr"))?;
        let expr = parse_scalar_expr(expr_node)?;
        collect_columns_from_scalar_expr(&expr, &mut cols);
        let alias = if rt.name.is_empty() {
            derive_expr_name(&expr)
        } else {
            rt.name.clone()
        };
        exprs.push((expr, alias));
    }
    Ok((Selection::Columns(cols), Some(exprs)))
}

pub(crate) fn from_item_is_join(node: &pg_query::Node) -> bool {
    matches!(node.node.as_ref(), Some(NodeEnum::JoinExpr(_)))
}

pub(crate) fn parse_from_item(node: pg_query::Node) -> PgWireResult<Plan> {
    use pg_query::NodeEnum;
    let n = node.node.ok_or_else(|| fe("missing FROM item"))?;
    match n {
        NodeEnum::RangeVar(rv) => {
            let schema = if rv.schemaname.is_empty() {
                None
            } else {
                Some(SchemaName::new(rv.schemaname))
            };
            let table = ObjName {
                schema,
                name: rv.relname,
            };
            let alias = rv.alias.and_then(|a| {
                if a.aliasname.is_empty() {
                    None
                } else {
                    Some(a.aliasname)
                }
            });
            Ok(Plan::UnboundSeqScan {
                table,
                alias,
                selection: Selection::Star,
                lock: None,
            })
        }
        NodeEnum::JoinExpr(j) => {
            let jt = pg_query::protobuf::JoinType::try_from(j.jointype)
                .unwrap_or(pg_query::protobuf::JoinType::JoinInner);
            let join_type = match jt {
                pg_query::protobuf::JoinType::JoinInner => JoinType::Inner,
                pg_query::protobuf::JoinType::JoinLeft => JoinType::Left,
                _ => return Err(fe_code("0A000", "only INNER and LEFT JOIN are supported")),
            };
            if !j.using_clause.is_empty() {
                return Err(fe_code(
                    "0A000",
                    "JOIN ... USING (...) is not supported yet",
                ));
            }
            let left_node = *j.larg.ok_or_else(|| fe("join missing left"))?;
            let right_node = *j.rarg.ok_or_else(|| fe("join missing right"))?;
            let left_plan = parse_from_item(left_node)?;
            let right_plan = parse_from_item(right_node)?;
            let on_expr_node = j.quals.and_then(|n| n.node);
            let on_bool = if let Some(nn) = on_expr_node {
                Some(parse_bool_expr(&nn)?)
            } else {
                None
            };
            Ok(Plan::UnboundJoin {
                left: Box::new(left_plan),
                right: Box::new(right_plan),
                join_type,
                on: on_bool,
            })
        }
        NodeEnum::RangeSubselect(rs) => {
            let sub_node = rs
                .subquery
                .ok_or_else(|| fe("missing subquery"))?
                .node
                .ok_or_else(|| fe("bad subquery"))?;
            let NodeEnum::SelectStmt(sel) = sub_node else {
                return Err(fe("only SELECT subqueries are supported in FROM"));
            };
            let plan = plan_select(*sel)?;
            let alias = rs.alias.and_then(|alias| {
                if alias.aliasname.is_empty() {
                    None
                } else {
                    Some(AliasSpec {
                        alias: alias.aliasname,
                        column_names: alias
                            .colnames
                            .into_iter()
                            .filter_map(|column| match column.node {
                                Some(NodeEnum::String(column)) => Some(column.sval),
                                _ => None,
                            })
                            .collect(),
                    })
                }
            });
            if let Some(alias) = alias {
                Ok(Plan::Alias {
                    input: Box::new(plan),
                    alias,
                    schema: Schema { fields: vec![] },
                })
            } else {
                Ok(plan)
            }
        }
        NodeEnum::RangeFunction(function) => plan_range_function(function),
        _ => Err(fe("unsupported FROM item")),
    }
}

pub(crate) fn plan_range_function(
    function: pg_query::protobuf::RangeFunction,
) -> PgWireResult<Plan> {
    if function.ordinality || function.is_rowsfrom || function.functions.len() != 1 {
        return Err(fe("unsupported set-returning function shape"));
    }
    let function_entry = function.functions.into_iter().next().unwrap();
    let Some(NodeEnum::List(function_entry)) = function_entry.node else {
        return Err(fe("invalid set-returning function"));
    };
    let Some(NodeEnum::FuncCall(call)) = function_entry
        .items
        .into_iter()
        .next()
        .and_then(|node| node.node)
    else {
        return Err(fe("invalid set-returning function"));
    };
    let name = call
        .funcname
        .iter()
        .filter_map(|part| match part.node.as_ref() {
            Some(NodeEnum::String(part)) => Some(part.sval.as_str()),
            _ => None,
        })
        .next_back()
        .ok_or_else(|| fe("set-returning function requires a name"))?;
    if name == "pg_input_error_info" {
        let input = call
            .args
            .first()
            .and_then(|argument| argument.node.as_ref())
            .and_then(|argument| match argument {
                NodeEnum::AConst(value) => match value.val.as_ref() {
                    Some(Val::Sval(value)) => Some(value.sval.clone()),
                    _ => None,
                },
                _ => None,
            })
            .ok_or_else(|| fe("pg_input_error_info() requires text constants"))?;
        let data_type = call
            .args
            .get(1)
            .and_then(|argument| argument.node.as_ref())
            .and_then(|argument| match argument {
                NodeEnum::AConst(value) => match value.val.as_ref() {
                    Some(Val::Sval(value)) => Some(value.sval.as_str()),
                    _ => None,
                },
                _ => None,
            })
            .unwrap_or("path");
        let error = match data_type {
            "path" => crate::engine::parse_path_text(&input).err(),
            "lseg" => crate::engine::parse_lseg_text(&input).err(),
            "line" => crate::engine::parse_line_text(&input).err(),
            "tid" => crate::engine::parse_tid_text(&input).err(),
            "oid" => crate::engine::parse_oid_text(&input).err(),
            "oidvector" => input.split_whitespace().find_map(|part| {
                crate::engine::parse_oid_text(part).err().map(|mut error| {
                    if error.code == "22P02" {
                        let invalid =
                            part.trim_start_matches(|character: char| character.is_ascii_digit());
                        error.message = format!(
                            "invalid input syntax for type oid: \"{}\"",
                            if invalid.is_empty() { part } else { invalid }
                        );
                    }
                    error
                })
            }),
            "pg_lsn" => crate::engine::parse_pg_lsn_text(&input).err(),
            "macaddr" => crate::engine::parse_macaddr_text(&input).err(),
            "macaddr8" => crate::engine::parse_macaddr8_text(&input).err(),
            "time" => crate::engine::parse_time_text(&input, None).err(),
            data_type if data_type.starts_with("varchar(") => {
                crate::engine::validate_varchar_input(&input, data_type).err()
            }
            data_type if data_type.starts_with("char(") => {
                crate::engine::validate_char_input(&input, data_type).err()
            }
            _ => None,
        };
        let message = error.as_ref().map_or_else(
            || format!("invalid input syntax for type {data_type}: \"{input}\""),
            |error| error.message.clone(),
        );
        let code = error.as_ref().map_or("22P02", |error| error.code);
        return Ok(Plan::Values {
            rows: vec![vec![
                Expr::Literal(Value::Text(message)),
                Expr::Literal(Value::Null),
                Expr::Literal(Value::Null),
                Expr::Literal(Value::Text(code.to_string())),
            ]],
            schema: Schema {
                fields: ["message", "detail", "hint", "sql_error_code"]
                    .into_iter()
                    .map(|name| Field {
                        name: name.to_string(),
                        data_type: DataType::Text,
                        origin: None,
                    })
                    .collect(),
            },
        });
    }
    if name != "generate_series" || !(2..=3).contains(&call.args.len()) {
        return Err(fe("unsupported set-returning function"));
    }
    let mut args = Vec::with_capacity(call.args.len());
    for arg in call.args {
        let Some(NodeEnum::AConst(arg)) = arg.node else {
            return Err(fe("generate_series() requires integer constants"));
        };
        let Some(Val::Ival(arg)) = arg.val else {
            return Err(fe("generate_series() requires integer constants"));
        };
        args.push(arg.ival as i64);
    }
    let (start, stop) = (args[0], args[1]);
    let step = args.get(2).copied().unwrap_or(1);
    if step == 0 {
        return Err(fe_code("22023", "step size cannot equal zero"));
    }
    let column_name = function
        .alias
        .as_ref()
        .and_then(|alias| alias.colnames.first())
        .and_then(|column| match column.node.as_ref() {
            Some(NodeEnum::String(column)) => Some(column.sval.clone()),
            _ => None,
        })
        .or_else(|| function.alias.map(|alias| alias.aliasname))
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| "generate_series".to_string());
    let mut rows = Vec::new();
    let mut value = start;
    while (step > 0 && value <= stop) || (step < 0 && value >= stop) {
        rows.push(vec![Expr::Literal(Value::Int64(value))]);
        value = value
            .checked_add(step)
            .ok_or_else(|| fe_code("22003", "integer out of range"))?;
    }
    Ok(Plan::Values {
        rows,
        schema: Schema {
            fields: vec![Field {
                name: column_name,
                data_type: DataType::Int4,
                origin: None,
            }],
        },
    })
}

pub(crate) fn parse_order_clause(clause: &[pg_query::Node]) -> PgWireResult<Vec<SortKey>> {
    let mut keys = Vec::with_capacity(clause.len());
    for sort in clause {
        let NodeEnum::SortBy(s) = sort.node.as_ref().ok_or_else(|| fe("bad order by"))? else {
            return Err(fe("bad order by node"));
        };
        let asc = match s.sortby_dir {
            1 | 2 => true,
            3 => false,
            4 => {
                let operator = s
                    .use_op
                    .iter()
                    .filter_map(|node| match node.node.as_ref() {
                        Some(NodeEnum::String(value)) => Some(value.sval.as_str()),
                        _ => None,
                    })
                    .next_back()
                    .ok_or_else(|| fe("ORDER BY USING requires an operator"))?;
                match operator {
                    "<" => true,
                    ">" => false,
                    _ => return Err(fe("unsupported ORDER BY USING operator")),
                }
            }
            _ => return Err(fe("bad ORDER BY direction")),
        };
        let nulls_first = match s.sortby_nulls {
            2 => Some(true),
            3 => Some(false),
            _ => None,
        };
        let Some(expr) = s.node.as_ref().and_then(|n| n.node.as_ref()) else {
            return Err(fe("bad order by expr"));
        };
        let key = match expr {
            NodeEnum::AConst(ac) => {
                if let Some(Val::Ival(iv)) = ac.val.as_ref() {
                    if iv.ival <= 0 {
                        return Err(fe("order by position must be >= 1"));
                    }
                    SortKey::ByIndex {
                        idx: (iv.ival as usize) - 1,
                        asc,
                        nulls_first,
                    }
                } else {
                    return Err(fe("order by const must be integer"));
                }
            }
            NodeEnum::ColumnRef(cr) => SortKey::ByName {
                col: parse_column_ref(cr)?.column,
                asc,
                nulls_first,
            },
            _ => {
                let expr = parse_scalar_expr(expr)?;
                SortKey::Expr {
                    expr,
                    asc,
                    nulls_first,
                }
            }
        };
        keys.push(key);
    }
    Ok(keys)
}

pub(crate) fn rewrite_order_keys_for_projection(
    keys: &mut [SortKey],
    exprs: &[(ScalarExpr, String)],
) {
    for key in keys {
        match key {
            SortKey::ByIndex {
                idx,
                asc,
                nulls_first,
            } => {
                if let Some((expr, _)) = exprs.get(*idx) {
                    *key = SortKey::Expr {
                        expr: expr.clone(),
                        asc: *asc,
                        nulls_first: *nulls_first,
                    };
                }
            }
            SortKey::ByName {
                col,
                asc,
                nulls_first,
            } => {
                if let Some((expr, _)) = exprs.iter().find(|(_, name)| name == col) {
                    *key = SortKey::Expr {
                        expr: expr.clone(),
                        asc: *asc,
                        nulls_first: *nulls_first,
                    };
                }
            }
            SortKey::Expr { .. } => {}
        }
    }
}

pub(crate) fn collect_columns_from_order_keys(keys: &[SortKey], out: &mut Vec<String>) {
    for key in keys {
        match key {
            SortKey::ByName { col, .. } => out.push(col.clone()),
            SortKey::Expr { expr, .. } => collect_columns_from_scalar_expr(expr, out),
            SortKey::ByIndex { .. } => {}
        }
    }
}

pub(crate) fn ensure_columns_present(
    cols: &mut Vec<String>,
    needed: Vec<String>,
    project_prefix_len: &mut Option<usize>,
) {
    let start_len = cols.len();
    let mut added = false;
    for col in needed {
        if !cols.contains(&col) {
            cols.push(col);
            added = true;
        }
    }
    if project_prefix_len.is_none() && added {
        *project_prefix_len = Some(start_len);
    }
}

pub(crate) fn parse_limit_count(node: &NodeEnum) -> PgWireResult<CountExpr> {
    parse_nonnegative_count(node, "limit")
}

pub(crate) fn parse_offset_count(node: &NodeEnum) -> PgWireResult<CountExpr> {
    parse_nonnegative_count(node, "offset")
}

pub(crate) fn parse_nonnegative_count(node: &NodeEnum, label: &str) -> PgWireResult<CountExpr> {
    match node {
        NodeEnum::AConst(c) => {
            if let Some(Val::Ival(iv)) = c.val.as_ref() {
                if iv.ival < 0 {
                    return Err(fe(format!("{label} must be non-negative")));
                }
                Ok(CountExpr::Value(iv.ival as usize))
            } else {
                Err(fe(format!("{label} must be integer")))
            }
        }
        NodeEnum::ParamRef(pr) => {
            if pr.number <= 0 {
                return Err(fe("parameter numbers start at 1"));
            }
            Ok(CountExpr::Expr(ScalarExpr::Param {
                idx: (pr.number as usize) - 1,
                ty: Some(DataType::Int8),
            }))
        }
        NodeEnum::TypeCast(tc) => {
            let inner = tc
                .arg
                .as_ref()
                .and_then(|n| n.node.as_ref())
                .ok_or_else(|| fe("bad type cast"))?;
            let cast_type = tc
                .type_name
                .as_ref()
                .ok_or_else(|| fe("missing cast target"))?;
            let dt = parse_type_name(cast_type)?;
            if !matches!(dt, DataType::Int2 | DataType::Int4 | DataType::Int8) {
                return Err(fe(format!("{label} must be integer")));
            }
            let count = parse_nonnegative_count(inner, label)?;
            Ok(apply_count_expr_param_type(count, dt))
        }
        _ => Err(fe(format!("unsupported {label} expression"))),
    }
}

pub(crate) fn apply_count_expr_param_type(expr: CountExpr, dt: DataType) -> CountExpr {
    match expr {
        CountExpr::Expr(ScalarExpr::Param { idx, .. }) => {
            CountExpr::Expr(ScalarExpr::Param { idx, ty: Some(dt) })
        }
        other => other,
    }
}

pub(crate) fn extract_col_name(rt: &ResTarget) -> PgWireResult<String> {
    let Some(v) = rt.val.as_ref().and_then(|n| n.node.as_ref()) else {
        return Err(fe("bad column target"));
    };
    if let NodeEnum::ColumnRef(cr) = v {
        Ok(parse_column_ref(cr)?.column)
    } else {
        Err(fe("only simple column names supported"))
    }
}

pub(crate) fn infer_expr_type(expr: &ScalarExpr) -> DataType {
    match expr {
        ScalarExpr::Literal(Value::Int64(_)) => DataType::Int8,
        ScalarExpr::Literal(Value::Float64Bits(_)) => DataType::Float8,
        ScalarExpr::Literal(Value::Text(_)) => DataType::Text,
        ScalarExpr::Literal(Value::Bool(_)) => DataType::Bool,
        ScalarExpr::Literal(Value::Date(_)) => DataType::Date,
        ScalarExpr::Literal(Value::TimestampMicros(_)) => DataType::Timestamp,
        ScalarExpr::Literal(Value::TimestamptzMicros(_)) => DataType::Timestamptz,
        ScalarExpr::Literal(Value::Bytes(_)) => DataType::Bytea,
        ScalarExpr::Cast { ty, .. } => ty.clone(),
        ScalarExpr::Predicate(_) => DataType::Bool,
        ScalarExpr::WindowRowNumber(_) => DataType::Int8,
        ScalarExpr::Subquery(plan) => plan
            .schema()
            .fields
            .first()
            .map(|f| f.data_type.clone())
            .unwrap_or(DataType::Text),
        ScalarExpr::Case {
            when_then,
            else_expr,
        } => when_then
            .iter()
            .map(|(_, result)| infer_expr_type(result))
            .next()
            .or_else(|| else_expr.as_ref().map(|expr| infer_expr_type(expr)))
            .unwrap_or(DataType::Text),
        _ => DataType::Text,
    }
}

pub(crate) fn infer_agg_type(agg: &AggCall) -> DataType {
    match agg.func {
        AggFunc::Count => DataType::Int8,
        AggFunc::Sum => match agg.expr.as_ref().map(infer_expr_type) {
            Some(DataType::Float8) => DataType::Float8,
            _ => DataType::Int8,
        },
        AggFunc::Avg => DataType::Float8,
        AggFunc::Min | AggFunc::Max => agg
            .expr
            .as_ref()
            .map(infer_expr_type)
            .unwrap_or(DataType::Text),
        AggFunc::BoolAnd => DataType::Bool,
    }
}
