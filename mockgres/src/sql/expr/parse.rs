use super::*;

pub fn parse_bool_expr(node: &NodeEnum) -> PgWireResult<BoolExpr> {
    parse_bool_expr_internal(node, None)
}

pub fn parse_bool_expr_with_aggregates(
    node: &NodeEnum,
    collector: &mut AggregateExprCollector,
) -> PgWireResult<BoolExpr> {
    parse_bool_expr_internal(node, Some(collector))
}

fn parse_bool_expr_internal(
    node: &NodeEnum,
    mut agg_ctx: Option<&mut AggregateExprCollector>,
) -> PgWireResult<BoolExpr> {
    match node {
        NodeEnum::BoolExpr(be) => {
            let op = BoolExprType::try_from(be.boolop).map_err(|_| fe("bad bool expr op"))?;
            let mut args = Vec::new();
            for a in &be.args {
                let n = a.node.as_ref().ok_or_else(|| fe("bad bool arg"))?;
                args.push(parse_bool_expr_internal(n, agg_ctx.as_deref_mut())?);
            }
            match op {
                BoolExprType::AndExpr => Ok(BoolExpr::And(args)),
                BoolExprType::OrExpr => Ok(BoolExpr::Or(args)),
                BoolExprType::NotExpr => {
                    if args.len() != 1 {
                        return Err(fe("NOT expects single operand"));
                    }
                    Ok(BoolExpr::Not(Box::new(args.into_iter().next().unwrap())))
                }
                BoolExprType::Undefined => Err(fe("unsupported bool op")),
            }
        }
        NodeEnum::AExpr(ax) => {
            let kind = AExprKind::try_from(ax.kind).map_err(|_| fe("unknown expression kind"))?;
            if kind == AExprKind::AexprIn {
                return parse_in_expr(ax, agg_ctx.as_deref_mut());
            }
            if kind == AExprKind::AexprOpAny {
                return parse_any_expr(ax, agg_ctx.as_deref_mut());
            }
            if ax.name.is_empty() {
                if let Some(inner) = ax.lexpr.as_ref().and_then(|n| n.node.as_ref()) {
                    return parse_bool_expr_internal(inner, agg_ctx);
                }
                return Err(fe("bad parenthesized expression"));
            }
            let op = parse_cmp_op(&ax.name)?;
            let lexpr = ax
                .lexpr
                .as_ref()
                .and_then(|n| n.node.as_ref())
                .ok_or_else(|| fe("bad where lhs"))?;
            let rexpr = ax
                .rexpr
                .as_ref()
                .and_then(|n| n.node.as_ref())
                .ok_or_else(|| fe("bad where rhs"))?;
            let lhs = parse_scalar_expr_internal(lexpr, agg_ctx.as_deref_mut())?;
            let rhs = parse_scalar_expr_internal(rexpr, agg_ctx.as_deref_mut())?;
            Ok(BoolExpr::Comparison { lhs, op, rhs })
        }
        NodeEnum::NullTest(nt) => {
            let nt_type =
                NullTestType::try_from(nt.nulltesttype).map_err(|_| fe("bad nulltest"))?;
            let arg = nt
                .arg
                .as_ref()
                .and_then(|n| n.node.as_ref())
                .ok_or_else(|| fe("bad nulltest arg"))?;
            let expr = parse_scalar_expr_internal(arg, agg_ctx)?;
            Ok(BoolExpr::IsNull {
                expr,
                negated: matches!(nt_type, NullTestType::IsNotNull),
            })
        }
        NodeEnum::SubLink(sl) => {
            let testexpr = sl
                .testexpr
                .as_ref()
                .and_then(|n| n.node.as_ref())
                .ok_or_else(|| fe("subquery requires test expression"))?;
            let lhs = parse_scalar_expr_internal(testexpr, agg_ctx.as_deref_mut())?;
            let subselect = sl
                .subselect
                .as_ref()
                .and_then(|n| n.node.as_ref())
                .ok_or_else(|| fe("subquery missing SELECT"))?;
            let NodeEnum::SelectStmt(sel) = subselect else {
                return Err(fe("only SELECT supported in subquery"));
            };
            let plan = crate::sql::dml::plan_select(*sel.clone())?;
            Ok(BoolExpr::InSubquery {
                expr: lhs,
                subplan: Box::new(plan),
            })
        }
        NodeEnum::AConst(c) => match const_to_value(c)? {
            Value::Bool(b) => Ok(BoolExpr::Literal(b)),
            _ => Err(fe("boolean literal expected")),
        },
        NodeEnum::ColumnRef(_)
        | NodeEnum::ParamRef(_)
        | NodeEnum::FuncCall(_)
        | NodeEnum::CoalesceExpr(_)
        | NodeEnum::CaseExpr(_)
        | NodeEnum::TypeCast(_) => {
            let expr = parse_scalar_expr_internal(node, agg_ctx.as_deref_mut())?;
            Ok(BoolExpr::Comparison {
                lhs: expr,
                op: CmpOp::Eq,
                rhs: ScalarExpr::Literal(Value::Bool(true)),
            })
        }
        _ => Err(fe("unsupported boolean expression")),
    }
}

pub fn parse_scalar_expr(node: &NodeEnum) -> PgWireResult<ScalarExpr> {
    parse_scalar_expr_internal(node, None)
}

pub fn parse_scalar_expr_with_aggregates(
    node: &NodeEnum,
    collector: &mut AggregateExprCollector,
) -> PgWireResult<ScalarExpr> {
    parse_scalar_expr_internal(node, Some(collector))
}

pub(super) fn parse_scalar_expr_internal(
    node: &NodeEnum,
    mut agg_ctx: Option<&mut AggregateExprCollector>,
) -> PgWireResult<ScalarExpr> {
    match node {
        NodeEnum::ColumnRef(cr) => Ok(ScalarExpr::Column(parse_column_ref(cr)?)),
        NodeEnum::ParamRef(pr) => parse_param_ref(pr),
        NodeEnum::AExpr(ax) => {
            if ax.name.is_empty() {
                let inner = ax
                    .lexpr
                    .as_ref()
                    .and_then(|n| n.node.as_ref())
                    .ok_or_else(|| fe("bad parenthesized scalar expression"))?;
                return parse_scalar_expr_internal(inner, agg_ctx);
            }
            let kind = AExprKind::try_from(ax.kind).map_err(|_| fe("unknown expression kind"))?;
            let is_unary_bit_not = ax.lexpr.is_none()
                && ax.name.iter().any(|name| {
                    matches!(name.node.as_ref(), Some(NodeEnum::String(name)) if name.sval == "~")
                });
            if is_unary_bit_not {
                return parse_arithmetic_expr(ax, agg_ctx.as_deref_mut());
            }
            let is_bool_expr = matches!(kind, AExprKind::AexprIn | AExprKind::AexprOpAny)
                || parse_cmp_op(&ax.name).is_ok();
            if is_bool_expr {
                let be = parse_bool_expr_internal(node, agg_ctx.as_deref_mut())?;
                Ok(ScalarExpr::Predicate(Box::new(be)))
            } else {
                parse_arithmetic_expr(ax, agg_ctx.as_deref_mut())
            }
        }
        NodeEnum::BoolExpr(_) | NodeEnum::NullTest(_) => {
            let be = parse_bool_expr_internal(node, agg_ctx.as_deref_mut())?;
            Ok(ScalarExpr::Predicate(Box::new(be)))
        }
        NodeEnum::FuncCall(fc) => parse_function_call(fc, agg_ctx.as_deref_mut()),
        NodeEnum::CoalesceExpr(ce) => parse_coalesce_expr(ce, agg_ctx.as_deref_mut()),
        NodeEnum::CaseExpr(ce) => parse_case_expr(ce, agg_ctx.as_deref_mut()),
        NodeEnum::SqlvalueFunction(svf) => parse_sql_value_function(svf),
        NodeEnum::SubLink(sl) => {
            let subselect = sl
                .subselect
                .as_ref()
                .and_then(|n| n.node.as_ref())
                .ok_or_else(|| fe("subquery missing SELECT"))?;
            let NodeEnum::SelectStmt(sel) = subselect else {
                return Err(fe("only SELECT supported in subquery"));
            };
            parse_scalar_subselect(sel, agg_ctx.as_deref_mut())
        }
        NodeEnum::TypeCast(tc) => {
            let inner = tc
                .arg
                .as_ref()
                .and_then(|n| n.node.as_ref())
                .ok_or_else(|| fe("bad type cast"))?;
            let target = tc
                .type_name
                .as_ref()
                .ok_or_else(|| fe("missing cast target"))?;
            let is_text = target.names.iter().any(|name| {
                matches!(
                    name.node.as_ref(),
                    Some(NodeEnum::String(name)) if name.sval.eq_ignore_ascii_case("text")
                )
            });
            let is_indtoast_row = match inner {
                NodeEnum::ColumnRef(column) => column.fields.iter().any(|field| {
                    matches!(
                        field.node.as_ref(),
                        Some(NodeEnum::String(name)) if name.sval == "indtoasttest"
                    )
                }),
                NodeEnum::FuncCall(call) => call.funcname.iter().any(|name| {
                    matches!(
                        name.node.as_ref(),
                        Some(NodeEnum::String(name)) if name.sval == "make_tuple_indirect"
                    )
                }),
                _ => false,
            };
            if is_text && is_indtoast_row {
                return Ok(ScalarExpr::Func {
                    func: ScalarFunc::IndirectToastRow,
                    args: ["descr", "cnt", "f1", "f2"]
                        .into_iter()
                        .map(|column| {
                            ScalarExpr::Column(ColumnRefName {
                                schema: None,
                                relation: Some("indtoasttest".to_string()),
                                column: column.to_string(),
                                location: None,
                            })
                        })
                        .collect(),
                });
            }
            let input_position = match inner {
                NodeEnum::AConst(value) => value.location,
                _ => tc.location,
            };
            let expr = parse_scalar_expr_internal(inner, agg_ctx.as_deref_mut())?;
            let is_name_array = !target.array_bounds.is_empty()
                && target.names.iter().any(|name| {
                    matches!(
                        name.node.as_ref(),
                        Some(NodeEnum::String(name)) if name.sval.eq_ignore_ascii_case("name")
                    )
                });
            if is_name_array
                && let ScalarExpr::Func {
                    func: ScalarFunc::ParseIdent,
                    args,
                } = expr
            {
                return Ok(ScalarExpr::Func {
                    func: ScalarFunc::ParseIdentNameArray,
                    args,
                });
            }
            let dt = parse_type_name(target)?;
            if dt == crate::engine::DataType::Tid
                && let ScalarExpr::Literal(Value::Text(value)) = &expr
                && let Err(error) = crate::engine::parse_tid_text(value)
            {
                let mut info =
                    ErrorInfo::new("ERROR".to_string(), error.code.to_string(), error.message);
                info.position = Some((input_position + 1).to_string());
                return Err(PgWireError::UserError(Box::new(info)));
            }
            if let ScalarExpr::Literal(Value::Text(value)) = &expr {
                let error = match dt {
                    crate::engine::DataType::MacAddr => {
                        crate::engine::parse_macaddr_text(value).err()
                    }
                    crate::engine::DataType::MacAddr8 => {
                        crate::engine::parse_macaddr8_text(value).err()
                    }
                    crate::engine::DataType::Time(precision) => {
                        crate::engine::parse_time_text(value, precision).err()
                    }
                    _ => None,
                };
                if let Some(error) = error {
                    let mut info =
                        ErrorInfo::new("ERROR".to_string(), error.code.to_string(), error.message);
                    info.position = Some((input_position + 1).to_string());
                    return Err(PgWireError::UserError(Box::new(info)));
                }
            }
            Ok(ScalarExpr::Cast {
                expr: Box::new(expr),
                ty: dt,
            })
        }
        NodeEnum::CollateClause(clause) => {
            let expression = clause
                .arg
                .as_ref()
                .and_then(|node| node.node.as_ref())
                .ok_or_else(|| fe("COLLATE requires an expression"))?;
            parse_scalar_expr_internal(expression, agg_ctx)
        }
        NodeEnum::MinMaxExpr(mm) => {
            let op = pg_query::protobuf::MinMaxOp::try_from(mm.op)
                .map_err(|_| fe("unsupported minmax op"))?;
            if op != pg_query::protobuf::MinMaxOp::IsGreatest {
                return Err(fe("only GREATEST is supported"));
            }
            if mm.args.is_empty() {
                return Err(fe("greatest() requires arguments"));
            }
            let mut args = Vec::new();
            for arg in &mm.args {
                let node = arg
                    .node
                    .as_ref()
                    .ok_or_else(|| fe("bad greatest argument"))?;
                args.push(parse_scalar_expr_internal(node, agg_ctx.as_deref_mut())?);
            }
            Ok(ScalarExpr::Func {
                func: ScalarFunc::Greatest,
                args,
            })
        }
        NodeEnum::XmlExpr(_) => Err(unsupported_xml_feature()),
        _ => {
            if let Some(v) = try_parse_literal(node)? {
                Ok(ScalarExpr::Literal(v))
            } else {
                Err(fe("unsupported scalar expression"))
            }
        }
    }
}

fn parse_scalar_subselect(
    sel: &SelectStmt,
    agg_ctx: Option<&mut AggregateExprCollector>,
) -> PgWireResult<ScalarExpr> {
    if !sel.from_clause.is_empty()
        || sel.where_clause.is_some()
        || !sel.group_clause.is_empty()
        || sel.having_clause.is_some()
        || !sel.sort_clause.is_empty()
        || sel.limit_count.is_some()
        || sel.limit_offset.is_some()
        || !sel.locking_clause.is_empty()
    {
        let plan = crate::sql::dml::plan_select(sel.clone())?;
        return Ok(ScalarExpr::Subquery(Box::new(plan)));
    }
    if sel.target_list.len() != 1 {
        return Err(fe("scalar subquery must select exactly one column"));
    }
    let target = sel.target_list.first().unwrap();
    let target_node = target
        .node
        .as_ref()
        .ok_or_else(|| fe("missing subquery target"))?;
    let NodeEnum::ResTarget(rt) = target_node else {
        return Err(fe("unsupported subquery target"));
    };
    let ResTarget { val, .. } = rt.as_ref();
    let expr_node = val
        .as_ref()
        .and_then(|n| n.node.as_ref())
        .ok_or_else(|| fe("missing subquery expression"))?;
    parse_scalar_expr_internal(expr_node, agg_ctx)
}

fn parse_coalesce_expr(
    ce: &CoalesceExpr,
    mut agg_ctx: Option<&mut AggregateExprCollector>,
) -> PgWireResult<ScalarExpr> {
    let mut args = Vec::new();
    for arg in &ce.args {
        let node = arg
            .node
            .as_ref()
            .ok_or_else(|| fe("bad coalesce argument"))?;
        args.push(parse_scalar_expr_internal(node, agg_ctx.as_deref_mut())?);
    }
    if args.is_empty() {
        return Err(fe("coalesce requires at least one argument"));
    }
    Ok(ScalarExpr::Func {
        func: ScalarFunc::Coalesce,
        args,
    })
}

fn parse_case_expr(
    ce: &CaseExpr,
    mut agg_ctx: Option<&mut AggregateExprCollector>,
) -> PgWireResult<ScalarExpr> {
    let case_operand = ce
        .arg
        .as_ref()
        .and_then(|n| n.node.as_ref())
        .map(|node| parse_scalar_expr_internal(node, agg_ctx.as_deref_mut()))
        .transpose()?;

    let mut when_then = Vec::with_capacity(ce.args.len());
    for arg in &ce.args {
        let node = arg.node.as_ref().ok_or_else(|| fe("bad CASE branch"))?;
        let NodeEnum::CaseWhen(cw) = node else {
            return Err(fe("bad CASE branch"));
        };
        let when_node = cw
            .expr
            .as_ref()
            .and_then(|n| n.node.as_ref())
            .ok_or_else(|| fe("missing CASE WHEN condition"))?;
        let condition = if let Some(case_operand) = case_operand.as_ref() {
            let rhs = parse_scalar_expr_internal(when_node, agg_ctx.as_deref_mut())?;
            BoolExpr::Comparison {
                lhs: case_operand.clone(),
                op: CmpOp::Eq,
                rhs,
            }
        } else {
            parse_bool_expr_internal(when_node, agg_ctx.as_deref_mut())?
        };
        let result_node = cw
            .result
            .as_ref()
            .and_then(|n| n.node.as_ref())
            .ok_or_else(|| fe("missing CASE THEN result"))?;
        let result = parse_scalar_expr_internal(result_node, agg_ctx.as_deref_mut())?;
        when_then.push((condition, result));
    }
    if when_then.is_empty() {
        return Err(fe("CASE requires at least one WHEN"));
    }
    let else_expr = ce
        .defresult
        .as_ref()
        .and_then(|n| n.node.as_ref())
        .map(|node| parse_scalar_expr_internal(node, agg_ctx.as_deref_mut()))
        .transpose()?
        .map(Box::new);
    Ok(ScalarExpr::Case {
        when_then,
        else_expr,
    })
}

fn parse_row_expr_items(
    node: &NodeEnum,
    mut agg_ctx: Option<&mut AggregateExprCollector>,
) -> PgWireResult<Option<Vec<ScalarExpr>>> {
    let args = match node {
        NodeEnum::RowExpr(row) => row.args.as_slice(),
        _ => return Ok(None),
    };
    let mut out = Vec::with_capacity(args.len());
    for arg in args {
        let node = arg.node.as_ref().ok_or_else(|| fe("bad row expression"))?;
        out.push(parse_scalar_expr_internal(node, agg_ctx.as_deref_mut())?);
    }
    Ok(Some(out))
}

fn parse_in_expr(
    ax: &AExpr,
    mut agg_ctx: Option<&mut AggregateExprCollector>,
) -> PgWireResult<BoolExpr> {
    let lexpr = ax
        .lexpr
        .as_ref()
        .and_then(|n| n.node.as_ref())
        .ok_or_else(|| fe("IN expression missing lhs"))?;
    if let Some(lhs_items) = parse_row_expr_items(lexpr, agg_ctx.as_deref_mut())? {
        return parse_tuple_in_expr(lhs_items, ax, agg_ctx.as_deref_mut());
    }
    let lhs = parse_scalar_expr_internal(lexpr, agg_ctx.as_deref_mut())?;
    let rexpr_node = ax
        .rexpr
        .as_ref()
        .and_then(|n| n.node.as_ref())
        .ok_or_else(|| fe("IN expression missing rhs"))?;
    match rexpr_node {
        NodeEnum::List(list) => {
            if list.items.is_empty() {
                return Err(fe("IN list must have at least one element"));
            }
            let mut comparisons = Vec::with_capacity(list.items.len());
            for item in &list.items {
                let node = item
                    .node
                    .as_ref()
                    .ok_or_else(|| fe("bad IN list element"))?;
                let rhs = parse_scalar_expr_internal(node, agg_ctx.as_deref_mut())?;
                comparisons.push(BoolExpr::Comparison {
                    lhs: lhs.clone(),
                    op: CmpOp::Eq,
                    rhs,
                });
            }
            if comparisons.len() == 1 {
                Ok(comparisons.pop().unwrap())
            } else {
                Ok(BoolExpr::Or(comparisons))
            }
        }
        NodeEnum::SubLink(sl) => {
            let subselect = sl
                .subselect
                .as_ref()
                .and_then(|n| n.node.as_ref())
                .ok_or_else(|| fe("IN subquery missing subselect"))?;
            let NodeEnum::SelectStmt(sel) = subselect else {
                return Err(fe("only SELECT supported in IN subquery"));
            };
            let plan = crate::sql::dml::plan_select(*sel.clone())?;
            Ok(BoolExpr::InSubquery {
                expr: lhs,
                subplan: Box::new(plan),
            })
        }
        _ => Err(fe("IN expression expects list or subquery")),
    }
}

fn parse_tuple_in_expr(
    lhs_items: Vec<ScalarExpr>,
    ax: &AExpr,
    mut agg_ctx: Option<&mut AggregateExprCollector>,
) -> PgWireResult<BoolExpr> {
    if lhs_items.is_empty() {
        return Err(fe("row IN expression requires at least one column"));
    }
    let rexpr_node = ax
        .rexpr
        .as_ref()
        .and_then(|n| n.node.as_ref())
        .ok_or_else(|| fe("IN expression missing rhs"))?;
    let NodeEnum::List(list) = rexpr_node else {
        return Err(fe("row IN expression expects list"));
    };
    if list.items.is_empty() {
        return Err(fe("IN list must have at least one element"));
    }

    let mut row_matches = Vec::with_capacity(list.items.len());
    for item in &list.items {
        let node = item
            .node
            .as_ref()
            .ok_or_else(|| fe("bad IN list element"))?;
        let rhs_items = parse_row_expr_items(node, agg_ctx.as_deref_mut())?
            .ok_or_else(|| fe("row IN expression expects row values"))?;
        if rhs_items.len() != lhs_items.len() {
            return Err(fe_code(
                "42601",
                "row IN expressions must have the same number of columns",
            ));
        }

        let mut comparisons = Vec::with_capacity(lhs_items.len());
        for (lhs, rhs) in lhs_items.iter().cloned().zip(rhs_items.into_iter()) {
            comparisons.push(BoolExpr::Comparison {
                lhs,
                op: CmpOp::Eq,
                rhs,
            });
        }
        if comparisons.len() == 1 {
            row_matches.push(comparisons.pop().unwrap());
        } else {
            row_matches.push(BoolExpr::And(comparisons));
        }
    }

    if row_matches.len() == 1 {
        Ok(row_matches.pop().unwrap())
    } else {
        Ok(BoolExpr::Or(row_matches))
    }
}

fn parse_any_expr(
    ax: &AExpr,
    mut agg_ctx: Option<&mut AggregateExprCollector>,
) -> PgWireResult<BoolExpr> {
    let op = parse_cmp_op(&ax.name)?;
    if op != CmpOp::Eq {
        return Err(fe("only = ANY is supported"));
    }
    let lexpr = ax
        .lexpr
        .as_ref()
        .and_then(|n| n.node.as_ref())
        .ok_or_else(|| fe("ANY expression missing lhs"))?;
    let lhs = parse_scalar_expr_internal(lexpr, agg_ctx.as_deref_mut())?;
    let rexpr = ax
        .rexpr
        .as_ref()
        .and_then(|n| n.node.as_ref())
        .ok_or_else(|| fe("ANY expression missing rhs"))?;

    let elements: &[pg_query::protobuf::Node] = match rexpr {
        NodeEnum::AArrayExpr(AArrayExpr { elements, .. }) => elements.as_slice(),
        NodeEnum::ArrayExpr(arr) => arr.elements.as_slice(),
        _ => return Err(fe("ANY expects array expression rhs")),
    };
    if elements.is_empty() {
        return Err(fe("ANY array must have elements"));
    }
    let mut comparisons = Vec::with_capacity(elements.len());
    for elem in elements {
        let node = elem.node.as_ref().ok_or_else(|| fe("bad ANY element"))?;
        let rhs = parse_scalar_expr_internal(node, agg_ctx.as_deref_mut())?;
        comparisons.push(BoolExpr::Comparison {
            lhs: lhs.clone(),
            op: CmpOp::Eq,
            rhs,
        });
    }
    if comparisons.len() == 1 {
        Ok(comparisons.pop().unwrap())
    } else {
        Ok(BoolExpr::Or(comparisons))
    }
}

fn parse_sql_value_function(svf: &SqlValueFunction) -> PgWireResult<ScalarExpr> {
    let op = SqlValueFunctionOp::try_from(svf.op).map_err(|_| fe("unknown SQL value function"))?;
    let func = match op {
        SqlValueFunctionOp::SvfopCurrentTimestamp | SqlValueFunctionOp::SvfopCurrentTimestampN => {
            ScalarFunc::CurrentTimestamp
        }
        SqlValueFunctionOp::SvfopCurrentDate => ScalarFunc::CurrentDate,
        _ => return Err(fe("unsupported SQL value function")),
    };
    Ok(ScalarExpr::Func {
        func,
        args: Vec::new(),
    })
}

fn parse_param_ref(pr: &ParamRef) -> PgWireResult<ScalarExpr> {
    if pr.number <= 0 {
        return Err(fe("parameter numbers start at 1"));
    }
    Ok(ScalarExpr::Param {
        idx: (pr.number as usize) - 1,
        ty: None,
    })
}
