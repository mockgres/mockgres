use crate::engine::{Plan, Schema, Field, Selection, fe};
use crate::db::Db;

pub fn bind(db: &Db, p: Plan) -> pgwire::error::PgWireResult<Plan> {
    match p {
        Plan::UnboundSeqScan { table, selection } => {
            let schema_name = table.schema.as_deref().unwrap_or("public");
            let tm = db
                .resolve_table(schema_name, &table.name)
                .map_err(|e| fe(e.to_string()))?;

            // build (idx, Field) for executor + compose output schema
            let cols: Vec<(usize, Field)> = match selection {
                Selection::Star => tm.columns.iter().enumerate().map(|(i, c)| {
                    (i, Field { name: c.name.clone(), data_type: c.data_type.clone() })
                }).collect(),
                Selection::Columns(names) => {
                    let mut out = Vec::with_capacity(names.len());
                    for n in names {
                        let i = tm.columns.iter().position(|c| c.name == n)
                            .ok_or_else(|| crate::engine::fe(format!("unknown column: {}", n)))?;
                        out.push((i, Field { name: n, data_type: tm.columns[i].data_type.clone() }));
                    }
                    out
                }
            };
            let schema = Schema { fields: cols.iter().map(|(_, f)| f.clone()).collect() };

            Ok(Plan::SeqScan { table, cols, schema })
        }

        // wrappers: bind child; nothing else to do
        Plan::Filter { input, pred, project_prefix_len } => {
            let child = bind(db, *input)?;
            Ok(Plan::Filter { input: Box::new(child), pred, project_prefix_len })
        }
        Plan::Order { input, keys } => {
            let child = bind(db, *input)?;
            Ok(Plan::Order { input: Box::new(child), keys })
        }
        Plan::Limit { input, limit } => {
            let child = bind(db, *input)?;
            Ok(Plan::Limit { input: Box::new(child), limit })
        }

        other => Ok(other),
    }
}
