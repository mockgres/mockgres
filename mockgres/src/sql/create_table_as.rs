use crate::catalog::SchemaName;
use crate::engine::{ObjName, Plan, fe, fe_code};
use pg_query::NodeEnum;
use pg_query::protobuf::{CreateTableAsStmt, ObjectType, OnCommitAction};
use pgwire::error::PgWireResult;

pub(super) fn plan_create_table_as(stmt: CreateTableAsStmt) -> PgWireResult<Plan> {
    let object_type = ObjectType::try_from(stmt.objtype)
        .map_err(|_| fe("unknown CREATE TABLE AS object type"))?;
    if object_type != ObjectType::ObjectTable {
        return Err(fe_code("0A000", "only CREATE TABLE AS is supported"));
    }
    if stmt.is_select_into {
        return Err(fe_code("0A000", "SELECT INTO is not supported"));
    }

    let into = stmt
        .into
        .ok_or_else(|| fe("CREATE TABLE AS requires a target table"))?;
    if !into.access_method.is_empty() {
        return Err(fe_code(
            "0A000",
            "CREATE TABLE AS access methods are not supported",
        ));
    }
    if !into.options.is_empty() {
        return Err(fe_code(
            "0A000",
            "CREATE TABLE AS storage options are not supported",
        ));
    }
    if !into.table_space_name.is_empty() {
        return Err(fe_code(
            "0A000",
            "CREATE TABLE AS tablespaces are not supported",
        ));
    }
    if into.view_query.is_some() {
        return Err(fe_code(
            "0A000",
            "CREATE TABLE AS view queries are not supported",
        ));
    }
    let on_commit =
        OnCommitAction::try_from(into.on_commit).map_err(|_| fe("unknown ON COMMIT action"))?;
    if !matches!(
        on_commit,
        OnCommitAction::Undefined | OnCommitAction::OncommitNoop
    ) {
        return Err(fe_code("0A000", "ON COMMIT is not supported"));
    }

    let relation = into
        .rel
        .ok_or_else(|| fe("CREATE TABLE AS requires a target table"))?;
    if !matches!(relation.relpersistence.as_str(), "" | "p") {
        return Err(fe_code(
            "0A000",
            "temporary and unlogged CREATE TABLE AS are not supported",
        ));
    }
    let table = ObjName {
        schema: (!relation.schemaname.is_empty()).then(|| SchemaName::new(relation.schemaname)),
        name: relation.relname,
    };

    let mut column_names = Vec::with_capacity(into.col_names.len());
    for column in into.col_names {
        let Some(NodeEnum::String(column)) = column.node else {
            return Err(fe("invalid CREATE TABLE AS column name"));
        };
        column_names.push(column.sval);
    }

    let query = stmt
        .query
        .and_then(|query| query.node)
        .ok_or_else(|| fe("CREATE TABLE AS requires a query"))?;
    let NodeEnum::SelectStmt(query) = query else {
        return Err(fe_code(
            "0A000",
            "only SELECT and VALUES are supported by CREATE TABLE AS",
        ));
    };

    Ok(Plan::CreateTableAs {
        table,
        column_names,
        query: Box::new(super::dml::plan_select(*query)?),
        with_data: !into.skip_data,
        if_not_exists: stmt.if_not_exists,
    })
}
