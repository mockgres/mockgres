use super::*;

pub(super) fn collect_from_relation_names<'a>(node: Option<&'a NodeEnum>, out: &mut Vec<&'a str>) {
    match node {
        Some(NodeEnum::RangeVar(relation)) => out.push(relation.relname.as_str()),
        Some(NodeEnum::JoinExpr(join)) => {
            collect_from_relation_names(
                join.larg.as_ref().and_then(|node| node.node.as_ref()),
                out,
            );
            collect_from_relation_names(
                join.rarg.as_ref().and_then(|node| node.node.as_ref()),
                out,
            );
        }
        _ => {}
    }
}

pub(super) fn ungrouped_column_error(
    column: Option<&crate::engine::ColumnRefName>,
    table: Option<&ObjName>,
) -> PgWireError {
    let qualified = column
        .map(|column| {
            let relation = column
                .relation
                .as_deref()
                .or_else(|| table.map(|table| table.name.as_str()));
            match relation {
                Some(relation) => format!("{relation}.{}", column.column),
                None => column.column.clone(),
            }
        })
        .unwrap_or_else(|| "column".to_string());
    let mut info = ErrorInfo::new(
        "ERROR".to_string(),
        "42803".to_string(),
        format!(
            "column \"{qualified}\" must appear in the GROUP BY clause or be used in an aggregate function"
        ),
    );
    info.position = column
        .and_then(|column| column.location)
        .map(|location| (location + 1).to_string());
    PgWireError::UserError(Box::new(info))
}

pub(super) fn first_column_in_scalar_expr(
    expr: &ScalarExpr,
) -> Option<&crate::engine::ColumnRefName> {
    match expr {
        ScalarExpr::Column(column) => Some(column),
        ScalarExpr::BinaryOp { left, right, .. } => {
            first_column_in_scalar_expr(left).or_else(|| first_column_in_scalar_expr(right))
        }
        ScalarExpr::UnaryOp { expr, .. } | ScalarExpr::Cast { expr, .. } => {
            first_column_in_scalar_expr(expr)
        }
        ScalarExpr::Func { args, .. } => args.iter().find_map(first_column_in_scalar_expr),
        ScalarExpr::Predicate(expr) => first_column_in_bool_expr(expr),
        ScalarExpr::Case {
            when_then,
            else_expr,
        } => when_then
            .iter()
            .find_map(|(condition, result)| {
                first_column_in_bool_expr(condition).or_else(|| first_column_in_scalar_expr(result))
            })
            .or_else(|| else_expr.as_deref().and_then(first_column_in_scalar_expr)),
        ScalarExpr::WindowRowNumber(spec) => spec
            .partition_by
            .iter()
            .find_map(first_column_in_scalar_expr),
        ScalarExpr::Literal(_)
        | ScalarExpr::ColumnIdx(_)
        | ScalarExpr::ExcludedIdx(_)
        | ScalarExpr::Param { .. }
        | ScalarExpr::Subquery(_) => None,
    }
}

pub(super) fn first_column_in_bool_expr(expr: &BoolExpr) -> Option<&crate::engine::ColumnRefName> {
    match expr {
        BoolExpr::Comparison { lhs, rhs, .. } => {
            first_column_in_scalar_expr(lhs).or_else(|| first_column_in_scalar_expr(rhs))
        }
        BoolExpr::And(parts) | BoolExpr::Or(parts) => {
            parts.iter().find_map(first_column_in_bool_expr)
        }
        BoolExpr::Not(expr) => first_column_in_bool_expr(expr),
        BoolExpr::IsNull { expr, .. }
        | BoolExpr::InSubquery { expr, .. }
        | BoolExpr::InListValues { expr, .. } => first_column_in_scalar_expr(expr),
        BoolExpr::Literal(_) => None,
    }
}

pub(super) fn parse_locking_clause(
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

pub(super) fn detect_count_star(node: &pg_query::Node) -> Option<String> {
    let rt = node.node.as_ref().and_then(|n| match n {
        NodeEnum::ResTarget(rt) => Some(rt),
        _ => None,
    })?;
    let expr_node = rt.val.as_ref()?.node.as_ref()?;
    let NodeEnum::FuncCall(fc) = expr_node else {
        return None;
    };
    if !fc.agg_star || fc.agg_distinct {
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

pub(super) fn target_list_contains_aggregates(target_list: &[pg_query::Node]) -> bool {
    use pg_query::NodeEnum;

    for t in target_list {
        let Some(NodeEnum::ResTarget(rt)) = t.node.as_ref() else {
            continue;
        };
        let Some(expr_node) = rt.val.as_ref().and_then(|n| n.node.as_ref()) else {
            continue;
        };
        if expr_node_contains_aggregate(expr_node) {
            if let NodeEnum::FuncCall(fc) = expr_node
                && target_list.len() == 1
                && fc.agg_star
                && !fc.agg_distinct
                && function_name(fc).is_some_and(|name| name.eq_ignore_ascii_case("count"))
            {
                continue;
            }
            return true;
        }
    }
    false
}

pub(super) fn function_name(fc: &pg_query::protobuf::FuncCall) -> Option<String> {
    fc.funcname
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
}

pub(super) fn expr_node_contains_aggregate(node: &NodeEnum) -> bool {
    match node {
        NodeEnum::FuncCall(fc) => {
            if function_name(fc).is_some_and(|name| is_aggregate_func_name(&name)) {
                return true;
            }
            fc.args
                .iter()
                .filter_map(|arg| arg.node.as_ref())
                .any(expr_node_contains_aggregate)
        }
        NodeEnum::CoalesceExpr(ce) => ce
            .args
            .iter()
            .filter_map(|arg| arg.node.as_ref())
            .any(expr_node_contains_aggregate),
        NodeEnum::AExpr(ax) => {
            ax.lexpr
                .as_ref()
                .and_then(|n| n.node.as_ref())
                .is_some_and(expr_node_contains_aggregate)
                || ax
                    .rexpr
                    .as_ref()
                    .and_then(|n| n.node.as_ref())
                    .is_some_and(expr_node_contains_aggregate)
        }
        NodeEnum::TypeCast(tc) => tc
            .arg
            .as_ref()
            .and_then(|n| n.node.as_ref())
            .is_some_and(expr_node_contains_aggregate),
        NodeEnum::MinMaxExpr(mm) => mm
            .args
            .iter()
            .filter_map(|arg| arg.node.as_ref())
            .any(expr_node_contains_aggregate),
        NodeEnum::AArrayExpr(arr) => arr
            .elements
            .iter()
            .filter_map(|arg| arg.node.as_ref())
            .any(expr_node_contains_aggregate),
        NodeEnum::ArrayExpr(arr) => arr
            .elements
            .iter()
            .filter_map(|arg| arg.node.as_ref())
            .any(expr_node_contains_aggregate),
        NodeEnum::BoolExpr(be) => be
            .args
            .iter()
            .filter_map(|arg| arg.node.as_ref())
            .any(expr_node_contains_aggregate),
        NodeEnum::NullTest(nt) => nt
            .arg
            .as_ref()
            .and_then(|n| n.node.as_ref())
            .is_some_and(expr_node_contains_aggregate),
        NodeEnum::List(list) => list
            .items
            .iter()
            .filter_map(|arg| arg.node.as_ref())
            .any(expr_node_contains_aggregate),
        NodeEnum::SubLink(_) => false,
        _ => false,
    }
}

pub(super) struct AggregateSelectItem {
    pub(super) expr: ScalarExpr,
    pub(super) alias: String,
    pub(super) contains_aggregate: bool,
}

pub(super) fn parse_aggregate_select_list(
    target_list: &mut Vec<pg_query::Node>,
) -> PgWireResult<AggregateSelectList> {
    use pg_query::NodeEnum;

    let mut collector = AggregateExprCollector::new("__select_agg");
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
        let agg_count_before = collector.agg_count();
        let expr = parse_scalar_expr_with_aggregates(expr_node, &mut collector)?;
        let contains_aggregate = collector.agg_count() > agg_count_before;
        let alias = if rt.name.is_empty() {
            if let NodeEnum::FuncCall(fc) = expr_node {
                function_name(fc).unwrap_or_else(|| derive_expr_name(&expr))
            } else {
                derive_expr_name(&expr)
            }
        } else {
            rt.name.clone()
        };
        items.push(AggregateSelectItem {
            expr,
            alias,
            contains_aggregate,
        });
    }
    Ok((items, collector.into_aggs()))
}

pub(super) fn parse_group_clause(
    group_clause: &[pg_query::Node],
    target_list: &[pg_query::Node],
) -> PgWireResult<Vec<(ScalarExpr, String)>> {
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
        let expr_ref = if let NodeEnum::AConst(constant) = expr_ref
            && let Some(Val::Ival(position)) = constant.val.as_ref()
        {
            let position = position.ival as usize;
            if position == 0 || position > target_list.len() {
                let mut info = ErrorInfo::new(
                    "ERROR".to_string(),
                    "42P10".to_string(),
                    format!("GROUP BY position {position} is not in select list"),
                );
                info.position =
                    (constant.location >= 0).then(|| (constant.location + 1).to_string());
                return Err(PgWireError::UserError(Box::new(info)));
            }
            target_list[position - 1]
                .node
                .as_ref()
                .and_then(|target| match target {
                    NodeEnum::ResTarget(target) => target.val.as_ref()?.node.as_ref(),
                    _ => None,
                })
                .ok_or_else(|| fe("GROUP BY position has no target"))?
        } else {
            expr_ref
        };
        let expr = parse_scalar_expr(expr_ref)?;
        if out
            .iter()
            .any(|(group, _)| group_expression_matches(group, &expr))
        {
            continue;
        }
        let alias = derive_expr_name(&expr);
        out.push((expr, alias));
    }
    Ok(out)
}

pub(super) fn find_group_expr_index(
    expr: &ScalarExpr,
    groups: &[(ScalarExpr, String)],
) -> Option<usize> {
    groups
        .iter()
        .position(|(group, _)| group_expression_matches(group, expr))
}

pub(super) fn group_expression_matches(left: &ScalarExpr, right: &ScalarExpr) -> bool {
    match (left, right) {
        (ScalarExpr::Column(left), ScalarExpr::Column(right)) => {
            left.column == right.column
                && (left.relation.is_none()
                    || right.relation.is_none()
                    || left.relation == right.relation)
                && (left.schema.is_none() || right.schema.is_none() || left.schema == right.schema)
        }
        (
            ScalarExpr::BinaryOp {
                op: left_op,
                left: left_left,
                right: left_right,
            },
            ScalarExpr::BinaryOp {
                op: right_op,
                left: right_left,
                right: right_right,
            },
        ) => {
            left_op == right_op
                && group_expression_matches(left_left, right_left)
                && group_expression_matches(left_right, right_right)
        }
        (
            ScalarExpr::Func {
                func: left_func,
                args: left_args,
            },
            ScalarExpr::Func {
                func: right_func,
                args: right_args,
            },
        ) => {
            left_func == right_func
                && left_args.len() == right_args.len()
                && left_args
                    .iter()
                    .zip(right_args)
                    .all(|(left, right)| group_expression_matches(left, right))
        }
        (
            ScalarExpr::Cast {
                expr: left,
                ty: left_type,
            },
            ScalarExpr::Cast {
                expr: right,
                ty: right_type,
            },
        ) => left_type == right_type && group_expression_matches(left, right),
        _ => left == right,
    }
}

pub(super) fn copy_column_locations(target: &mut ScalarExpr, source: &ScalarExpr) {
    match (target, source) {
        (ScalarExpr::Column(target), ScalarExpr::Column(source)) => {
            target.location = source.location;
        }
        (
            ScalarExpr::BinaryOp {
                left: target_left,
                right: target_right,
                ..
            },
            ScalarExpr::BinaryOp {
                left: source_left,
                right: source_right,
                ..
            },
        ) => {
            copy_column_locations(target_left, source_left);
            copy_column_locations(target_right, source_right);
        }
        (
            ScalarExpr::Func {
                args: target_args, ..
            },
            ScalarExpr::Func {
                args: source_args, ..
            },
        ) => {
            for (target, source) in target_args.iter_mut().zip(source_args) {
                copy_column_locations(target, source);
            }
        }
        _ => {}
    }
}

pub(super) fn rewrite_bool_expr_for_groups(expr: &mut BoolExpr, groups: &[(ScalarExpr, String)]) {
    match expr {
        BoolExpr::Comparison { lhs, rhs, .. } => {
            rewrite_scalar_expr_for_groups(lhs, groups);
            rewrite_scalar_expr_for_groups(rhs, groups);
        }
        BoolExpr::And(parts) | BoolExpr::Or(parts) => {
            for part in parts {
                rewrite_bool_expr_for_groups(part, groups);
            }
        }
        BoolExpr::Not(expr) => rewrite_bool_expr_for_groups(expr, groups),
        BoolExpr::IsNull { expr, .. }
        | BoolExpr::InSubquery { expr, .. }
        | BoolExpr::InListValues { expr, .. } => rewrite_scalar_expr_for_groups(expr, groups),
        BoolExpr::Literal(_) => {}
    }
}

fn rewrite_scalar_expr_for_groups(expr: &mut ScalarExpr, groups: &[(ScalarExpr, String)]) {
    if let Some(index) = find_group_expr_index(expr, groups) {
        *expr = ScalarExpr::ColumnIdx(index);
        return;
    }

    match expr {
        ScalarExpr::BinaryOp { left, right, .. } => {
            rewrite_scalar_expr_for_groups(left, groups);
            rewrite_scalar_expr_for_groups(right, groups);
        }
        ScalarExpr::UnaryOp { expr, .. } | ScalarExpr::Cast { expr, .. } => {
            rewrite_scalar_expr_for_groups(expr, groups);
        }
        ScalarExpr::Func { args, .. } => {
            for argument in args {
                rewrite_scalar_expr_for_groups(argument, groups);
            }
        }
        ScalarExpr::Predicate(predicate) => rewrite_bool_expr_for_groups(predicate, groups),
        ScalarExpr::Case {
            when_then,
            else_expr,
        } => {
            for (condition, result) in when_then {
                rewrite_bool_expr_for_groups(condition, groups);
                rewrite_scalar_expr_for_groups(result, groups);
            }
            if let Some(expr) = else_expr {
                rewrite_scalar_expr_for_groups(expr, groups);
            }
        }
        ScalarExpr::Column(_)
        | ScalarExpr::ColumnIdx(_)
        | ScalarExpr::ExcludedIdx(_)
        | ScalarExpr::Literal(_)
        | ScalarExpr::Param { .. }
        | ScalarExpr::Subquery(_)
        | ScalarExpr::WindowRowNumber(_) => {}
    }
}

pub(super) fn rewrite_order_keys_for_groups(keys: &mut [SortKey], groups: &[(ScalarExpr, String)]) {
    for key in keys {
        let replacement = match key {
            SortKey::ByName {
                col,
                asc,
                nulls_first,
            } => groups
                .iter()
                .position(|(_, alias)| alias == col)
                .map(|idx| SortKey::ByIndex {
                    idx,
                    asc: *asc,
                    nulls_first: *nulls_first,
                }),
            SortKey::Expr {
                expr,
                asc,
                nulls_first,
            } => groups
                .iter()
                .position(|(group, _)| group_expression_matches(group, expr))
                .map(|idx| SortKey::ByIndex {
                    idx,
                    asc: *asc,
                    nulls_first: *nulls_first,
                }),
            SortKey::ByIndex { .. } => None,
        };
        if let Some(replacement) = replacement {
            *key = replacement;
        }
    }
}
