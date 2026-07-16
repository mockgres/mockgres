use super::*;
use crate::engine::SetOpKind;
use pg_query::protobuf::SetOperation;

pub(crate) fn plan_set_operation(mut select: SelectStmt) -> PgWireResult<Plan> {
    let operation = match SetOperation::try_from(select.op) {
        Ok(SetOperation::SetopUnion) => SetOpKind::Union,
        Ok(SetOperation::SetopIntersect) => SetOpKind::Intersect,
        Ok(SetOperation::SetopExcept) => SetOpKind::Except,
        _ => return Err(fe("invalid set operation")),
    };
    if !select.locking_clause.is_empty() {
        return Err(fe_code(
            "0A000",
            "FOR UPDATE is not allowed with set operations",
        ));
    }
    let left = select
        .larg
        .take()
        .ok_or_else(|| fe("set operation is missing its left query"))?;
    let right = select
        .rarg
        .take()
        .ok_or_else(|| fe("set operation is missing its right query"))?;
    let mut plan = Plan::SetOperation {
        left: Box::new(plan_select(*left)?),
        right: Box::new(plan_select(*right)?),
        op: operation,
        all: select.all,
        schema: Schema { fields: vec![] },
    };

    if !select.sort_clause.is_empty() {
        plan = Plan::Order {
            input: Box::new(plan),
            keys: parse_order_clause(&select.sort_clause)?,
        };
    }

    let limit = select
        .limit_count
        .as_ref()
        .and_then(|node| node.node.as_ref())
        .map(parse_limit_count)
        .transpose()?;
    let offset = select
        .limit_offset
        .as_ref()
        .and_then(|node| node.node.as_ref())
        .map(parse_offset_count)
        .transpose()?
        .unwrap_or(CountExpr::Value(0));
    if limit.is_some() || !matches!(offset, CountExpr::Value(0)) {
        plan = Plan::Limit {
            input: Box::new(plan),
            limit,
            offset,
        };
    }
    Ok(plan)
}
