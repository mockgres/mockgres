use crate::catalog::{SchemaName, TableId};
#[allow(unused_imports)]
use crate::engine::{
    AggCall, AggFunc, AliasSpec, BoolExpr, DataType, Field, JoinType, LockMode, LockRequest,
    LockSpec, ObjName, OnConflictAction, OnConflictTarget, Plan, ScalarExpr, Schema, Selection,
    SortKey, Value, fe, fe_code,
};
use pg_query::NodeEnum;
use pg_query::protobuf::a_const::Val;
use pg_query::protobuf::{LockClauseStrength, LockWaitPolicy, ResTarget, SelectStmt};
use pgwire::error::PgWireResult;

use super::expr::{
    AggregateExprCollector, agg_func_from_name, collect_columns_from_bool_expr,
    collect_columns_from_scalar_expr, derive_expr_name, is_aggregate_func_name, parse_bool_expr,
    parse_bool_expr_with_aggregates, parse_column_ref, parse_scalar_expr,
};

pub fn plan_select(mut sel: SelectStmt) -> PgWireResult<Plan> {
    if sel.from_clause.is_empty() {
        return plan_literal_select(sel);
    }
    let mut count_star = false;
    let mut count_alias = "count".to_string();
    if sel.target_list.len() == 1 {
        if let Some(alias) = detect_count_star(sel.target_list.first().unwrap()) {
            count_star = true;
            count_alias = alias;
        }
    }
    let has_other_aggs = target_list_contains_aggregates(&sel.target_list);
    let from_count = sel.from_clause.len();
    let has_join = sel.from_clause.iter().any(|node| from_item_is_join(node));
    let multi_from = from_count > 1 || has_join;
    let lock_request = parse_locking_clause(&mut sel.locking_clause, multi_from)?;

    let where_expr = if let Some(w) = sel.where_clause.as_ref().and_then(|n| n.node.as_ref()) {
        Some(parse_bool_expr(w)?)
    } else {
        None
    };
    let mut having_aggs = Vec::new();
    let mut having_expr = if let Some(h) = sel.having_clause.as_ref().and_then(|n| n.node.as_ref())
    {
        let mut collector = AggregateExprCollector::new("__having_agg");
        let expr = parse_bool_expr_with_aggregates(h, &mut collector)?;
        having_aggs = collector.into_aggs();
        Some(expr)
    } else {
        None
    };
    let has_having = having_expr.is_some();

    let selection_needs_projection =
        !count_star && !has_other_aggs && sel.group_clause.is_empty() && !has_having;
    let (mut selection, projection_items) = if selection_needs_projection {
        parse_select_list(&mut sel.target_list)?
    } else {
        (Selection::Star, None)
    };

    let mut project_prefix_len: Option<usize> = None;
    if !multi_from {
        if let (Selection::Columns(cols), Some(expr)) = (&mut selection, &where_expr) {
            let mut needed = Vec::new();
            collect_columns_from_bool_expr(expr, &mut needed);
            ensure_columns_present(cols, needed, &mut project_prefix_len);
        }
    }

    let mut order_keys: Option<Vec<SortKey>> = None;
    if !sel.sort_clause.is_empty() {
        let mut keys = parse_order_clause(&sel.sort_clause)?;
        if let Some(items) = &projection_items {
            rewrite_order_keys_for_projection(&mut keys, items);
        }
        if !multi_from {
            if let Selection::Columns(cols) = &mut selection {
                let mut needed = Vec::new();
                collect_columns_from_order_keys(&keys, &mut needed);
                ensure_columns_present(cols, needed, &mut project_prefix_len);
            }
        }
        order_keys = Some(keys);
    }

    if lock_request.is_some() {
        project_prefix_len = None;
    }

    let mut from_nodes = sel.from_clause;
    let mut plan = parse_from_item(from_nodes.remove(0))?;
    let mut first_table: Option<ObjName> = None;
    if !multi_from {
        plan = match plan {
            Plan::UnboundSeqScan { table, alias, .. } => {
                first_table = Some(table.clone());
                Plan::UnboundSeqScan {
                    table,
                    alias,
                    selection,
                    lock: lock_request,
                }
            }
            other => other,
        };
    }
    for item in from_nodes {
        let right = parse_from_item(item)?;
        plan = Plan::UnboundJoin {
            left: Box::new(plan),
            right: Box::new(right),
            join_type: JoinType::Inner,
            on: None,
        };
    }

    if let Some(pred) = where_expr {
        plan = Plan::Filter {
            input: Box::new(plan),
            expr: pred,
            project_prefix_len,
        };
    }

    if let Some(keys) = order_keys {
        plan = Plan::Order {
            input: Box::new(plan),
            keys,
        };
    }

    if lock_request.is_some() && count_star {
        return Err(fe_code(
            "0A000",
            "FOR UPDATE with aggregates is not supported",
        ));
    }

    if let Some(req) = lock_request {
        let first_table = first_table.ok_or_else(|| {
            fe_code(
                "0A000",
                "FOR UPDATE is only supported for single-table SELECT statements",
            )
        })?;
        plan = Plan::LockRows {
            table: first_table,
            input: Box::new(plan),
            lock: LockSpec {
                mode: req.mode,
                skip_locked: req.skip_locked,
                nowait: req.nowait,
                target: TableId {
                    schema_id: 0,
                    rel_id: 0,
                },
            },
            row_id_idx: 0,
            schema: Schema { fields: vec![] },
        };
    }

    let mut limit_value = None;
    if let Some(limit_node) = sel.limit_count.as_ref().and_then(|n| n.node.as_ref()) {
        limit_value = Some(parse_limit_count(limit_node)?);
    }
    let mut offset_value = 0usize;
    if let Some(offset_node) = sel.limit_offset.as_ref().and_then(|n| n.node.as_ref()) {
        offset_value = parse_offset_count(offset_node)?;
    }
    if limit_value.is_some() || offset_value != 0 {
        plan = Plan::Limit {
            input: Box::new(plan),
            limit: limit_value,
            offset: offset_value,
        };
    }

    if count_star && !has_other_aggs && sel.group_clause.is_empty() && !has_having {
        let schema = Schema {
            fields: vec![Field {
                name: count_alias,
                data_type: DataType::Int8,
                origin: None,
            }],
        };
        plan = Plan::CountRows {
            input: Box::new(plan),
            schema,
        };
    } else if has_other_aggs || !sel.group_clause.is_empty() || has_having {
        let group_clause_exprs = parse_group_clause(&sel.group_clause)?;
        let group_len = group_clause_exprs.len();
        let items = parse_aggregate_select_list(&mut sel.target_list)?;
        let mut select_proj = Vec::new();
        let mut select_agg_exprs = Vec::new();
        for item in items {
            if let Some((agg_call, alias)) = item.agg_call {
                let agg_idx = select_agg_exprs.len();
                select_proj.push(SelectProjection::Agg {
                    agg_idx,
                    alias: alias.clone(),
                });
                select_agg_exprs.push((agg_call, alias));
            } else if let Some((expr, alias)) = item.group_expr {
                if group_clause_exprs.is_empty() {
                    return Err(fe_code(
                        "42803",
                        "column must appear in the GROUP BY clause or be used in an aggregate function",
                    ));
                }
                let Some(idx) = find_group_expr_index(&expr, &group_clause_exprs) else {
                    return Err(fe_code(
                        "42803",
                        "column must appear in the GROUP BY clause or be used in an aggregate function",
                    ));
                };
                select_proj.push(SelectProjection::Group {
                    group_idx: idx,
                    alias,
                });
            }
        }
        if select_proj.is_empty() {
            return Err(fe("SELECT list is empty"));
        }
        if group_clause_exprs.is_empty()
            && select_proj
                .iter()
                .any(|proj| matches!(proj, SelectProjection::Group { .. }))
        {
            return Err(fe_code(
                "42803",
                "column must appear in the GROUP BY clause or be used in an aggregate function",
            ));
        }

        let mut agg_exprs_full = select_agg_exprs.clone();
        agg_exprs_full.extend(having_aggs.clone());

        let mut fields = Vec::new();
        for (expr, alias) in &group_clause_exprs {
            fields.push(Field {
                name: alias.clone(),
                data_type: infer_expr_type(expr),
                origin: None,
            });
        }
        for (agg, alias) in &agg_exprs_full {
            fields.push(Field {
                name: alias.clone(),
                data_type: infer_agg_type(agg),
                origin: None,
            });
        }

        let mut aggregate_plan = Plan::Aggregate {
            input: Box::new(plan),
            group_exprs: group_clause_exprs.clone(),
            agg_exprs: agg_exprs_full,
            schema: Schema { fields },
        };
        if let Some(expr) = having_expr.take() {
            aggregate_plan = Plan::Filter {
                input: Box::new(aggregate_plan),
                expr,
                project_prefix_len: None,
            };
        }

        let mut projection_exprs = Vec::new();
        let mut projection_fields = Vec::new();
        for proj in select_proj {
            match proj {
                SelectProjection::Group { group_idx, alias } => {
                    projection_exprs.push((ScalarExpr::ColumnIdx(group_idx), alias.clone()));
                    let dt = infer_expr_type(&group_clause_exprs[group_idx].0);
                    projection_fields.push(Field {
                        name: alias,
                        data_type: dt,
                        origin: None,
                    });
                }
                SelectProjection::Agg { agg_idx, alias } => {
                    let column_idx = group_len + agg_idx;
                    projection_exprs.push((ScalarExpr::ColumnIdx(column_idx), alias.clone()));
                    let dt = infer_agg_type(&select_agg_exprs[agg_idx].0);
                    projection_fields.push(Field {
                        name: alias,
                        data_type: dt,
                        origin: None,
                    });
                }
            }
        }
        plan = Plan::Projection {
            input: Box::new(aggregate_plan),
            exprs: projection_exprs,
            schema: Schema {
                fields: projection_fields,
            },
        };
    } else if let Some(exprs) = projection_items {
        let schema = Schema {
            fields: exprs
                .iter()
                .map(|(expr, name)| Field {
                    name: name.clone(),
                    data_type: infer_expr_type(expr),
                    origin: None,
                })
                .collect(),
        };
        plan = Plan::Projection {
            input: Box::new(plan),
            exprs,
            schema,
        };
    }

    Ok(plan)
}

fn parse_locking_clause(
    locking_clause: &mut Vec<pg_query::Node>,
    multi_from: bool,
) -> PgWireResult<Option<LockRequest>> {
    if locking_clause.is_empty() {
        return Ok(None);
    }
    if multi_from {
        return Err(fe_code(
            "0A000",
            "FOR UPDATE is only supported for single-table SELECT statements",
        ));
    }
    if locking_clause.len() != 1 {
        return Err(fe_code(
            "0A000",
            "only one locking clause is supported per SELECT",
        ));
    }
    let clause_node = locking_clause
        .remove(0)
        .node
        .ok_or_else(|| fe("missing locking clause"))?;
    let NodeEnum::LockingClause(clause) = clause_node else {
        return Err(fe("malformed locking clause"));
    };
    if !clause.locked_rels.is_empty() {
        return Err(fe_code(
            "0A000",
            "locking specific relations is not supported",
        ));
    }
    let strength =
        LockClauseStrength::try_from(clause.strength).map_err(|_| fe("bad locking strength"))?;
    if strength != LockClauseStrength::LcsForupdate {
        return Err(fe_code("0A000", "only FOR UPDATE is supported"));
    }
    let wait_policy =
        LockWaitPolicy::try_from(clause.wait_policy).map_err(|_| fe("bad wait policy"))?;
    let (skip_locked, nowait) = match wait_policy {
        LockWaitPolicy::LockWaitBlock | LockWaitPolicy::Undefined => (false, false),
        LockWaitPolicy::LockWaitSkip => (true, false),
        LockWaitPolicy::LockWaitError => (false, true),
    };
    Ok(Some(LockRequest {
        mode: LockMode::Update,
        skip_locked,
        nowait,
    }))
}

fn detect_count_star(node: &pg_query::Node) -> Option<String> {
    let rt = node.node.as_ref().and_then(|n| match n {
        NodeEnum::ResTarget(rt) => Some(rt),
        _ => None,
    })?;
    let expr_node = rt.val.as_ref()?.node.as_ref()?;
    let NodeEnum::FuncCall(fc) = expr_node else {
        return None;
    };
    if !fc.agg_star {
        return None;
    }
    let name = fc.funcname.iter().find_map(|n| {
        n.node.as_ref().and_then(|nn| {
            if let NodeEnum::String(s) = nn {
                Some(s.sval.to_ascii_lowercase())
            } else {
                None
            }
        })
    })?;
    if name != "count" {
        return None;
    }
    if rt.name.is_empty() {
        Some("count".into())
    } else {
        Some(rt.name.clone())
    }
}

fn target_list_contains_aggregates(target_list: &[pg_query::Node]) -> bool {
    use pg_query::NodeEnum;

    for t in target_list {
        let Some(NodeEnum::ResTarget(rt)) = t.node.as_ref() else {
            continue;
        };
        let Some(expr_node) = rt.val.as_ref().and_then(|n| n.node.as_ref()) else {
            continue;
        };
        if let NodeEnum::FuncCall(fc) = expr_node {
            let name = fc
                .funcname
                .iter()
                .find_map(|n| {
                    n.node.as_ref().and_then(|nn| {
                        if let NodeEnum::String(s) = nn {
                            Some(s.sval.clone())
                        } else {
                            None
                        }
                    })
                })
                .unwrap_or_default();
            if is_aggregate_func_name(&name) {
                if target_list.len() == 1 && fc.agg_star && name.eq_ignore_ascii_case("count") {
                    continue;
                }
                return true;
            }
        }
    }
    false
}

struct AggregateSelectItem {
    group_expr: Option<(ScalarExpr, String)>,
    agg_call: Option<(AggCall, String)>,
}

enum SelectProjection {
    Group { group_idx: usize, alias: String },
    Agg { agg_idx: usize, alias: String },
}

fn parse_aggregate_select_list(
    target_list: &mut Vec<pg_query::Node>,
) -> PgWireResult<Vec<AggregateSelectItem>> {
    use pg_query::NodeEnum;

    let mut items = Vec::new();
    for node in target_list.drain(..) {
        let rt = node
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
        if let NodeEnum::FuncCall(fc) = expr_node {
            let func_name = fc
                .funcname
                .iter()
                .find_map(|n| {
                    n.node.as_ref().and_then(|nn| {
                        if let NodeEnum::String(s) = nn {
                            Some(s.sval.to_ascii_lowercase())
                        } else {
                            None
                        }
                    })
                })
                .unwrap_or_default();
            if let Some(func) = agg_func_from_name(&func_name) {
                if fc.agg_distinct {
                    return Err(fe("DISTINCT aggregates are not supported"));
                }
                let alias = if rt.name.is_empty() {
                    func_name.clone()
                } else {
                    rt.name.clone()
                };
                let agg_call = if func == AggFunc::Count && fc.agg_star {
                    AggCall { func, expr: None }
                } else {
                    if fc.args.len() != 1 {
                        return Err(fe("aggregate functions require exactly one argument"));
                    }
                    let arg_node = fc.args[0]
                        .node
                        .as_ref()
                        .ok_or_else(|| fe("bad aggregate argument"))?;
                    let expr = parse_scalar_expr(arg_node)?;
                    AggCall {
                        func,
                        expr: Some(expr),
                    }
                };
                items.push(AggregateSelectItem {
                    group_expr: None,
                    agg_call: Some((agg_call, alias)),
                });
                continue;
            }
        }
        let expr = parse_scalar_expr(expr_node)?;
        let alias = if rt.name.is_empty() {
            derive_expr_name(&expr)
        } else {
            rt.name.clone()
        };
        items.push(AggregateSelectItem {
            group_expr: Some((expr, alias)),
            agg_call: None,
        });
    }
    Ok(items)
}

fn parse_group_clause(group_clause: &[pg_query::Node]) -> PgWireResult<Vec<(ScalarExpr, String)>> {
    use pg_query::NodeEnum;

    let mut out = Vec::with_capacity(group_clause.len());
    for node in group_clause {
        let Some(expr_node) = node.node.as_ref() else {
            return Err(fe("bad GROUP BY expression"));
        };
        let expr_ref = match expr_node {
            NodeEnum::SortBy(sort) => sort
                .node
                .as_ref()
                .and_then(|n| n.node.as_ref())
                .ok_or_else(|| fe("bad GROUP BY expression"))?,
            other => other,
        };
        let expr = parse_scalar_expr(expr_ref)?;
        let alias = derive_expr_name(&expr);
        out.push((expr, alias));
    }
    Ok(out)
}

fn find_group_expr_index(expr: &ScalarExpr, groups: &[(ScalarExpr, String)]) -> Option<usize> {
    groups.iter().position(|(gexpr, _)| gexpr == expr)
}

fn plan_literal_select(sel: SelectStmt) -> PgWireResult<Plan> {
    let tl = sel.target_list;
    if tl.is_empty() {
        return Err(fe("at least one column required"));
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
    let input = Plan::Values {
        rows: vec![vec![]],
        schema: Schema { fields: vec![] },
    };
    Ok(Plan::Projection {
        input: Box::new(input),
        exprs: out_exprs,
        schema: Schema { fields: vec![] },
    })
}

fn parse_select_list(
    target_list: &mut Vec<pg_query::Node>,
) -> PgWireResult<(Selection, Option<Vec<(ScalarExpr, String)>>)> {
    if target_list.len() == 1 {
        let t = &target_list[0];
        let node = t.node.as_ref().ok_or_else(|| fe("missing target node"))?;
        if let NodeEnum::ResTarget(rt) = node {
            if let Some(NodeEnum::ColumnRef(cr)) = rt.val.as_ref().and_then(|n| n.node.as_ref()) {
                if cr
                    .fields
                    .get(0)
                    .and_then(|f| f.node.as_ref())
                    .map(|n| matches!(n, NodeEnum::AStar(_)))
                    .unwrap_or(false)
                {
                    return Ok((Selection::Star, None));
                }
            }
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

fn from_item_is_join(node: &pg_query::Node) -> bool {
    matches!(node.node.as_ref(), Some(NodeEnum::JoinExpr(_)))
}

pub(super) fn parse_from_item(node: pg_query::Node) -> PgWireResult<Plan> {
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
            let alias = rs.alias.and_then(|a| {
                if a.aliasname.is_empty() {
                    None
                } else {
                    Some(a.aliasname)
                }
            });
            if let Some(a) = alias {
                Ok(Plan::Alias {
                    input: Box::new(plan),
                    alias: AliasSpec { alias: a },
                    schema: Schema { fields: vec![] },
                })
            } else {
                Ok(plan)
            }
        }
        _ => Err(fe("unsupported FROM item")),
    }
}

fn parse_order_clause(clause: &[pg_query::Node]) -> PgWireResult<Vec<SortKey>> {
    let mut keys = Vec::with_capacity(clause.len());
    for sort in clause {
        let NodeEnum::SortBy(s) = sort.node.as_ref().ok_or_else(|| fe("bad order by"))? else {
            return Err(fe("bad order by node"));
        };
        let asc = match s.sortby_dir {
            1 | 2 => true,
            3 => false,
            _ => true,
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

fn rewrite_order_keys_for_projection(keys: &mut [SortKey], exprs: &[(ScalarExpr, String)]) {
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

fn collect_columns_from_order_keys(keys: &[SortKey], out: &mut Vec<String>) {
    for key in keys {
        match key {
            SortKey::ByName { col, .. } => out.push(col.clone()),
            SortKey::Expr { expr, .. } => collect_columns_from_scalar_expr(expr, out),
            SortKey::ByIndex { .. } => {}
        }
    }
}

fn ensure_columns_present(
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

fn parse_limit_count(node: &NodeEnum) -> PgWireResult<usize> {
    parse_nonnegative_count(node, "limit")
}

fn parse_offset_count(node: &NodeEnum) -> PgWireResult<usize> {
    parse_nonnegative_count(node, "offset")
}

fn parse_nonnegative_count(node: &NodeEnum, label: &str) -> PgWireResult<usize> {
    match node {
        NodeEnum::AConst(c) => {
            if let Some(Val::Ival(iv)) = c.val.as_ref() {
                if iv.ival < 0 {
                    return Err(fe(format!("{label} must be non-negative")));
                }
                Ok(iv.ival as usize)
            } else {
                Err(fe(format!("{label} must be integer")))
            }
        }
        _ => Err(fe(format!("unsupported {label} expression"))),
    }
}

pub(super) fn extract_col_name(rt: &ResTarget) -> PgWireResult<String> {
    let Some(v) = rt.val.as_ref().and_then(|n| n.node.as_ref()) else {
        return Err(fe("bad column target"));
    };
    if let NodeEnum::ColumnRef(cr) = v {
        Ok(parse_column_ref(cr)?.column)
    } else {
        Err(fe("only simple column names supported"))
    }
}

fn infer_expr_type(expr: &ScalarExpr) -> DataType {
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
        _ => DataType::Text,
    }
}

fn infer_agg_type(agg: &AggCall) -> DataType {
    match agg.func {
        AggFunc::Count => DataType::Int8,
        AggFunc::Sum => match agg.expr.as_ref().map(|e| infer_expr_type(e)) {
            Some(DataType::Float8) => DataType::Float8,
            _ => DataType::Int8,
        },
        AggFunc::Avg => DataType::Float8,
        AggFunc::Min | AggFunc::Max => agg
            .expr
            .as_ref()
            .map(|e| infer_expr_type(e))
            .unwrap_or(DataType::Text),
    }
}
