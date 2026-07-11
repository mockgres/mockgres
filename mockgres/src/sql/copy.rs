use crate::catalog::SchemaName;
use crate::engine::{ObjName, Plan, fe, fe_code};
use pg_query::NodeEnum;
use pg_query::protobuf::CopyStmt;
use pgwire::error::PgWireResult;

pub(super) fn plan_copy(stmt: CopyStmt) -> PgWireResult<Plan> {
    if !stmt.is_from {
        return Err(fe_code("0A000", "COPY TO is not supported"));
    }
    if stmt.is_program {
        return Err(fe_code("0A000", "COPY FROM PROGRAM is not supported"));
    }
    if stmt.filename.is_empty() {
        return Err(fe_code("0A000", "COPY FROM STDIN is not supported"));
    }
    if stmt.query.is_some() {
        return Err(fe_code("0A000", "COPY queries are not supported"));
    }
    if stmt.where_clause.is_some() {
        return Err(fe_code("0A000", "COPY FROM WHERE is not supported"));
    }
    if !stmt.options.is_empty() {
        return Err(fe_code("0A000", "COPY options are not supported"));
    }

    let relation = stmt
        .relation
        .ok_or_else(|| fe("COPY FROM requires a table"))?;
    let table = ObjName {
        schema: (!relation.schemaname.is_empty()).then(|| SchemaName::new(relation.schemaname)),
        name: relation.relname,
    };
    let columns = if stmt.attlist.is_empty() {
        None
    } else {
        let mut columns = Vec::with_capacity(stmt.attlist.len());
        for attribute in stmt.attlist {
            let Some(NodeEnum::String(attribute)) = attribute.node else {
                return Err(fe("invalid COPY column"));
            };
            columns.push(attribute.sval);
        }
        Some(columns)
    };

    Ok(Plan::CopyFrom {
        table,
        columns,
        filename: stmt.filename,
    })
}
