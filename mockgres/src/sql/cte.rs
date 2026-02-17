use crate::engine::{CommonTableExprPlan, Plan, fe, fe_code};
use pg_query::NodeEnum;
use pg_query::protobuf::{CteMaterialize, WithClause};
use pgwire::error::PgWireResult;

use super::{delete::plan_delete, dml::plan_select, insert::plan_insert, update::plan_update};

pub(crate) fn wrap_with_clause(with_clause: Option<WithClause>, body: Plan) -> PgWireResult<Plan> {
    let Some(with_clause) = with_clause else {
        return Ok(body);
    };
    if with_clause.recursive {
        return Err(fe_code("0A000", "WITH RECURSIVE is not supported"));
    }
    let mut ctes = Vec::with_capacity(with_clause.ctes.len());
    for cte_node in with_clause.ctes {
        let node = cte_node.node.ok_or_else(|| fe("malformed CTE"))?;
        let NodeEnum::CommonTableExpr(cte) = node else {
            return Err(fe("malformed CTE"));
        };
        if cte.search_clause.is_some() || cte.cycle_clause.is_some() {
            return Err(fe_code(
                "0A000",
                "WITH SEARCH/CYCLE clauses are not supported",
            ));
        }
        if cte.cterecursive {
            return Err(fe_code("0A000", "WITH RECURSIVE is not supported"));
        }
        let materialized = CteMaterialize::try_from(cte.ctematerialized)
            .unwrap_or(CteMaterialize::CtematerializeUndefined);
        if matches!(materialized, CteMaterialize::Always | CteMaterialize::Never) {
            return Err(fe_code(
                "0A000",
                "CTE materialization options are not supported",
            ));
        }
        let name = cte.ctename;
        let output_columns = if cte.aliascolnames.is_empty() {
            None
        } else {
            let mut aliases = Vec::with_capacity(cte.aliascolnames.len());
            for col in cte.aliascolnames {
                let alias_node = col
                    .node
                    .as_ref()
                    .ok_or_else(|| fe("bad CTE column alias"))?;
                let NodeEnum::String(s) = alias_node else {
                    return Err(fe("bad CTE column alias"));
                };
                aliases.push(s.sval.clone());
            }
            Some(aliases)
        };
        let query_node = cte.ctequery.ok_or_else(|| fe("missing CTE query"))?;
        let query = query_node.node.ok_or_else(|| fe("missing CTE query"))?;
        let cte_plan = match query {
            NodeEnum::SelectStmt(sel) => plan_select(*sel)?,
            NodeEnum::InsertStmt(ins) => plan_insert(*ins)?,
            NodeEnum::UpdateStmt(upd) => plan_update(*upd)?,
            NodeEnum::DeleteStmt(del) => plan_delete(*del)?,
            _ => return Err(fe_code("0A000", "CTE query must be a SELECT statement")),
        };
        ctes.push(CommonTableExprPlan {
            name,
            plan: Box::new(cte_plan),
            output_columns,
            schema: None,
        });
    }

    Ok(Plan::With {
        ctes,
        body: Box::new(body),
    })
}
