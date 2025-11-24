use crate::catalog::SchemaId;
use crate::db::Db;
use crate::engine::{
    ColumnRefName, DataType, Field, ReturningClause, ReturningExpr, ScalarExpr, Schema,
};
use pgwire::error::PgWireResult;

use super::BindTimeContext;
use super::expr::{bind_scalar_expr, scalar_expr_type};

pub(crate) fn bind_returning_clause(
    clause: &mut ReturningClause,
    schema: &Schema,
    db: &Db,
    search_path: &[SchemaId],
    current_database: Option<&str>,
    time_ctx: BindTimeContext,
) -> PgWireResult<Schema> {
    let mut expanded = Vec::new();
    for item in clause.exprs.drain(..) {
        match item {
            ReturningExpr::Star => {
                for field in &schema.fields {
                    expanded.push(ReturningExpr::Expr {
                        expr: ScalarExpr::Column(ColumnRefName {
                            schema: None,
                            relation: None,
                            column: field.name.clone(),
                        }),
                        alias: field.name.clone(),
                    });
                }
            }
            ReturningExpr::Expr { expr, alias } => {
                expanded.push(ReturningExpr::Expr { expr, alias });
            }
        }
    }
    clause.exprs = expanded;
    let mut fields = Vec::with_capacity(clause.exprs.len());
    for item in clause.exprs.iter_mut() {
        if let ReturningExpr::Expr { expr, alias } = item {
            let bound = bind_scalar_expr(
                expr,
                schema,
                None,
                db,
                search_path,
                current_database,
                time_ctx,
            )?;
            let dt = scalar_expr_type(&bound, schema).unwrap_or(DataType::Text);
            fields.push(Field {
                name: alias.clone(),
                data_type: dt,
                origin: None,
            });
            *expr = bound;
        }
    }
    Ok(Schema { fields })
}
