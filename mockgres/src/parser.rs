use crate::engine::{fe, FilterPred, SortKey};
use crate::engine::{DataType, Expr, Field, ObjName, Plan, Schema, Selection, Value};
use pg_query::protobuf::AConst;
use pg_query::protobuf::a_const::Val;
use pg_query::{NodeEnum, parse};

pub struct Planner;

impl Planner {
    pub fn plan_sql(sql: &str) -> pgwire::error::PgWireResult<Plan> {
        let parsed = parse(sql).map_err(|e| pgwire::error::PgWireError::ApiError(Box::new(e)))?;
        // filter out empty statements produced by trailing semicolons.
        let stmts: Vec<_> = parsed
            .protobuf
            .stmts
            .into_iter()
            .filter(|s| s.stmt.is_some())
            .collect();

        // after filtering, ensure we have exactly one statement.
        if stmts.len() > 1 {
            return Err(fe("multiple statements not supported"));
        }

        let Some(stmt) = stmts.into_iter().next() else {
            // handle cases like a query with only a semicolon.
            return Err(fe("empty query"));
        };
        match stmt.stmt.and_then(|n| n.node) {
            // SELECT
            Some(NodeEnum::SelectStmt(sel)) => {
                // SELECT <literal>[, <literal>...]
                if sel.from_clause.is_empty() {
                    let tl = sel.target_list;
                    if tl.is_empty() {
                        return Err(fe("at least one column required"));
                    }

                    let mut out_fields = Vec::with_capacity(tl.len());
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

                        let lit = parse_numeric_const(expr_node)?;
                        let (dt, expr) = match lit {
                            Value::Int64(i) => {
                                let dt = if (i32::MIN as i64..=i32::MAX as i64).contains(&i) {
                                    DataType::Int4
                                } else {
                                    DataType::Int8
                                };
                                (dt, Expr::Literal(Value::Int64(i)))
                            }
                            Value::Float64Bits(b) => {
                                (DataType::Float8, Expr::Literal(Value::Float64Bits(b)))
                            }
                            Value::Null => {
                                // null has no inherent numeric type; bail in numeric-only contexts
                                return Err(fe("null not allowed here"));
                            }
                        };
                        // postgres uses "?column?" for unlabeled expressions
                        let name = if rt.name.is_empty() {
                            "?column?".to_string()
                        } else {
                            rt.name.clone()
                        };
                        out_fields.push(Field {
                            name: name.clone(),
                            data_type: dt,
                        });
                        out_exprs.push((expr, name));
                    }

                    let input = Plan::Values {
                        rows: vec![vec![]],
                        schema: Schema { fields: vec![] },
                    };
                    let out_schema = Schema { fields: out_fields };
                    return Ok(Plan::Projection {
                        input: Box::new(input),
                        exprs: out_exprs,
                        schema: out_schema,
                    });
                }

                // SELECT FROM
                if sel.from_clause.len() != 1 {
                    return Err(fe("single table only"));
                }
                let from = sel.from_clause[0]
                    .node
                    .as_ref()
                    .ok_or_else(|| fe("missing from"))?;
                let (schemaname, relname) = if let NodeEnum::RangeVar(rv) = from {
                    (
                        if rv.schemaname.is_empty() {
                            None
                        } else {
                            Some(rv.schemaname.clone())
                        },
                        rv.relname.clone(),
                    )
                } else {
                    return Err(fe("unsupported FROM"));
                };
                let table = ObjName {
                    schema: schemaname,
                    name: relname,
                };

                // star vs explicit columns
                let mut selection = if sel.target_list.len() == 1 {
                    let t = &sel.target_list[0];
                    let node = t.node.as_ref().ok_or_else(|| fe("missing target node"))?;

                    if let NodeEnum::ResTarget(rt) = node {
                        // select * case
                        if let Some(NodeEnum::ColumnRef(cr)) =
                            rt.val.as_ref().and_then(|n| n.node.as_ref())
                        {
                            if let Some(NodeEnum::AStar(_)) =
                                cr.fields.get(0).and_then(|f| f.node.as_ref())
                            {
                                Selection::Star
                            } else {
                                // single column, ie select a from ...
                                let name = if rt.name.is_empty() {
                                    extract_col_name(rt)?
                                } else {
                                    rt.name.clone()
                                };
                                Selection::Columns(vec![name])
                            }
                        } else {
                            // treat as single column name
                            let name = if rt.name.is_empty() {
                                extract_col_name(rt)?
                            } else {
                                rt.name.clone()
                            };
                            Selection::Columns(vec![name])
                        }
                    } else {
                        return Err(fe("unexpected target type for single column"));
                    }
                } else {
                    // multiple columns, ie select a, b from 
                    let mut cols = Vec::new();
                    for t in sel.target_list {
                        let node_enum = t.node.as_ref().map(|n| &*n).ok_or_else(|| fe("bad target"))?;
                        if let NodeEnum::ResTarget(rt) = node_enum {
                            let col_name = if rt.name.is_empty() {
                                extract_col_name(&rt)?
                            } else {
                                rt.name.clone()
                            };
                            cols.push(col_name);
                        } else {
                            return Err(fe("bad target"));
                        }
                    }
                    Selection::Columns(cols)
                };

                // parse where predicate now so we can add missing column to selection if needed
                let where_pred = if let Some(w) = sel.where_clause.as_ref().and_then(|n| n.node.as_ref()) {
                    parse_where_predicate(w)?
                } else { None };

                // if where uses a name not in selection and we're not doing star, include it
                if let Some(FilterPred::ByName { col, .. }) = &where_pred {
                    if let Selection::Columns(cols) = &mut selection {
                        if !cols.iter().any(|c| c == col) {
                            cols.push(col.clone());
                        }
                    }
                }

                // start with unbound scan, then wrap in filter → order → limit
                let mut plan = Plan::UnboundSeqScan { table, selection };

                // where
                if let Some(pred) = where_pred {
                    plan = Plan::Filter { input: Box::new(plan), pred, project_prefix_len: None };
                }

                // order by: ordinals or column names
                if !sel.sort_clause.is_empty() {
                    let keys = parse_order_clause(&sel.sort_clause)?;
                    plan = Plan::Order { input: Box::new(plan), keys };
                }

                // limit: integer constant
                if let Some(limit_node) = sel.limit_count.as_ref().and_then(|n| n.node.as_ref()) {
                    let lim = parse_limit_count(limit_node)?;
                    plan = Plan::Limit { input: Box::new(plan), limit: lim };
                }

                Ok(plan)
            }

            // CREATE TABLE
            Some(NodeEnum::CreateStmt(cs)) => {
                let rv = cs.relation.ok_or_else(|| fe("missing table name"))?;
                let table = ObjName {
                    schema: if rv.schemaname.is_empty() {
                        None
                    } else {
                        Some(rv.schemaname)
                    },
                    name: rv.relname,
                };

                let mut cols = Vec::new();
                let mut pk: Option<Vec<String>> = None;

                for elt in cs.table_elts {
                    match elt.node.unwrap() {
                        NodeEnum::ColumnDef(cd) => {
                            let dt = map_type(&cd)?;
                            let cname = cd.colname;
                            let nullable = !cd.is_not_null;
                            cols.push((cname, dt, nullable));
                            // simple “col PRIMARY KEY” handling
                            if cd.constraints.iter().any(|c| matches!(c.node.as_ref(), Some(NodeEnum::Constraint(cons)) if cons.contype==1)) {
                                pk = Some(vec![cols.last().unwrap().0.clone()]);
                            }
                        }
                        NodeEnum::Constraint(cons) => {
                            // PRIMARY KEY (a,b)
                            if cons.contype == 1 {
                                let mut names = Vec::new();
                                for n in cons.keys {
                                    let NodeEnum::String(s) = n.node.unwrap() else {
                                        continue;
                                    };
                                    names.push(s.sval);
                                }
                                pk = Some(names);
                            }
                        }
                        _ => {}
                    }
                }

                Ok(Plan::CreateTable { table, cols, pk })
            }

            // INSERT VALUES
            Some(NodeEnum::InsertStmt(ins)) => {
                let rv = ins.relation.ok_or_else(|| fe("missing target table"))?;
                let table = ObjName {
                    schema: if rv.schemaname.is_empty() {
                        None
                    } else {
                        Some(rv.schemaname)
                    },
                    name: rv.relname,
                };
                let sel = ins
                    .select_stmt
                    .and_then(|n| n.node)
                    .ok_or_else(|| fe("INSERT needs VALUES"))?;
                let NodeEnum::SelectStmt(sel2) = sel else {
                    return Err(fe("only VALUES supported"));
                };
                let mut all_rows: Vec<Vec<Expr>> = Vec::new();
                for v in sel2.values_lists {
                    let NodeEnum::List(vlist) = v.node.unwrap() else {
                        continue;
                    };
                    let mut row = Vec::new();
                    for cell in vlist.items {
                        let n = cell.node.unwrap();
                        match n {
                            NodeEnum::AConst(c) => row.push(Expr::Literal(const_to_value(&c)?)),
                            _ => return Err(fe("INSERT supports constants only")),
                        }
                    }
                    all_rows.push(row);
                }
                Ok(Plan::InsertValues {
                    table,
                    rows: all_rows,
                })
            }

            _ => Err(fe("unsupported statement")),
        }
    }
}

fn plan_schema(p: &Plan) -> pgwire::error::PgWireResult<Schema> {
    Ok(p.schema().clone())
}

// parse a where predicate supporting "col <op> const" only
fn parse_where_predicate(node: &NodeEnum) -> pgwire::error::PgWireResult<Option<FilterPred>> {
    use crate::engine::CmpOp;
    if let NodeEnum::AExpr(ax) = node {
        // parse operator text
        let mut op_txt = None;
        for n in &ax.name {
            if let Some(NodeEnum::String(s)) = n.node.as_ref() {
                op_txt = Some(s.sval.as_str());
                break;
            }
        }
        let op = match op_txt.unwrap_or("") {
            "="  => CmpOp::Eq,
            "!=" => CmpOp::Neq,
            "<>" => CmpOp::Neq,
            "<"  => CmpOp::Lt,
            "<=" => CmpOp::Lte,
            ">"  => CmpOp::Gt,
            ">=" => CmpOp::Gte,
            _ => return Err(fe("unsupported where operator")),
        };

        // expect left is column, right is const
        let lexpr = ax.lexpr.as_ref().and_then(|n| n.node.as_ref())
            .ok_or_else(|| fe("bad where aexpr"))?;
        let rexpr = ax.rexpr.as_ref().and_then(|n| n.node.as_ref())
            .ok_or_else(|| fe("bad where aexpr"))?;

        let col_name = match lexpr {
            NodeEnum::ColumnRef(cr) => last_colref_name(cr)?,
            _ => return Err(fe("where must be column op const")),
        };
        let rhs = match rexpr {
            NodeEnum::AConst(c) => const_to_value(c)?,
            _ => return Err(fe("where rhs must be const")),
        };
        return Ok(Some(FilterPred::ByName { col: col_name, op, rhs }));
    }
    Ok(None)
}

// get last identifier from a columnref
fn last_colref_name(cr: &pg_query::protobuf::ColumnRef) -> pgwire::error::PgWireResult<String> {
    for n in cr.fields.iter().rev() {
        if let Some(NodeEnum::String(s)) = n.node.as_ref() {
            return Ok(s.sval.clone());
        }
    }
    Err(fe("bad columnref"))
}

fn parse_order_clause(items: &Vec<pg_query::protobuf::Node>) -> pgwire::error::PgWireResult<Vec<SortKey>> {
    let mut keys = Vec::new();
    for sb in items {
        let Some(NodeEnum::SortBy(s)) = sb.node.as_ref() else { return Err(fe("bad order by")); };
        let asc = match s.sortby_dir {
            0 | 1 => true,  // default/asc
            2 => false,     // desc
            _ => true,
        };
        let Some(expr) = s.node.as_ref().and_then(|n| n.node.as_ref()) else { return Err(fe("bad order by expr")); };
        let key = match expr {
            // order by 1
            NodeEnum::AConst(ac) => {
                if let Some(Val::Ival(iv)) = ac.val.as_ref() {
                    if iv.ival <= 0 { return Err(fe("order by position must be >= 1")); }
                    SortKey::ByIndex { idx: (iv.ival as usize) - 1, asc }
                } else { return Err(fe("order by const must be integer")); }
            }
            // order by column
            NodeEnum::ColumnRef(cr) => {
                let name = last_colref_name(cr)?; SortKey::ByName { col: name, asc }
            }
            _ => return Err(fe("unsupported order by expression")),
        };
        keys.push(key);
    }
    Ok(keys)
}

fn parse_limit_count(node: &NodeEnum) -> pgwire::error::PgWireResult<usize> {
    match node {
        NodeEnum::AConst(c) => {
            if let Some(Val::Ival(iv)) = c.val.as_ref() {
                if iv.ival < 0 { return Err(fe("limit must be non-negative")); }
                Ok(iv.ival as usize)
            } else {
                Err(fe("limit must be integer"))
            }
        }
        _ => Err(fe("unsupported limit expression")),
    }
}


fn extract_col_name(rt: &pg_query::protobuf::ResTarget) -> pgwire::error::PgWireResult<String> {
    // SELECT a FROM t  (val is ColumnRef)
    let Some(v) = rt.val.as_ref().and_then(|n| n.node.as_ref()) else {
        return Err(fe("bad column target"));
    };
    if let NodeEnum::ColumnRef(cr) = v {
        let last = cr
            .fields
            .last()
            .and_then(|n| n.node.as_ref())
            .ok_or_else(|| fe("bad colref"))?;
        if let NodeEnum::String(s) = last {
            Ok(s.sval.clone())
        } else {
            Err(fe("bad colref"))
        }
    } else {
        Err(fe("only simple column names supported"))
    }
}

fn parse_numeric_const(node: &NodeEnum) -> pgwire::error::PgWireResult<Value> {
    match node {
        NodeEnum::AConst(c) => {
            let v = const_to_value(c)?;
            match v {
                Value::Int64(_) | Value::Float64Bits(_) => Ok(v),
                Value::Null => Err(fe("null not allowed in numeric const")),
            }
        }
        NodeEnum::AExpr(ax) => {
            // support unary minus over numeric constants: -123 or -1.23
            let is_minus = ax
                .name
                .iter()
                .any(|nn| matches!(nn.node.as_ref(), Some(NodeEnum::String(s)) if s.sval=="-"));
            if !is_minus {
                return Err(fe("only numeric const supported"));
            }
            let rhs = ax
                .rexpr
                .as_ref()
                .and_then(|n| n.node.as_ref())
                .ok_or_else(|| fe("bad unary minus"))?;
            match rhs {
                NodeEnum::AConst(c) => match const_to_value(c)? {
                    Value::Int64(i) => Ok(Value::Int64(-i)),
                    Value::Float64Bits(b) => Ok(Value::from_f64(-f64::from_bits(b))),
                    Value::Null => Err(fe("minus over null")),
                },
                _ => Err(fe("minus over non-const")),
            }
        }
        _ => Err(fe("only numeric const supported")),
    }
}

fn const_to_value(c: &AConst) -> pgwire::error::PgWireResult<Value> {
    // postgres encodes NULL as aconst with no val; treat it as null
    if c.val.is_none() {
        return Ok(Value::Null);
    }
    let v = c.val.as_ref().unwrap();
    match v {
        Val::Ival(i) => Ok(Value::Int64(i.ival as i64)),
        Val::Fval(f) => {
            Ok(Value::from_f64(f.fval.parse::<f64>().map_err(|e| {
                pgwire::error::PgWireError::ApiError(Box::new(e))
            })?))
        }
        Val::Boolval(_) => Err(fe("bool const not yet supported")),
        Val::Sval(_) => Err(fe("string const not yet supported")),
        Val::Bsval(_) => Err(fe("bitstring const not yet supported")),
    }
}

fn map_type(cd: &pg_query::protobuf::ColumnDef) -> pgwire::error::PgWireResult<DataType> {
    // subset: int/int4/integer, bigint/int8, float8/double precision
    let typ = cd.type_name.as_ref().ok_or_else(|| fe("missing type"))?;

    // collect tokens, keeping only string components, lowercased
    let tokens: Vec<String> = typ.names.iter().filter_map(|n| {
        n.node.as_ref().and_then(|nn| if let NodeEnum::String(s) = nn { Some(s.sval.to_ascii_lowercase()) } else { None })
    }).collect();

    if tokens.is_empty() {
        return Err(fe("bad type name"));
    }

    // handle multi-word names like "double precision"
    let last = tokens.last().unwrap().as_str();
    let last_two = if tokens.len() >= 2 {
        Some(format!("{} {}", tokens[tokens.len()-2], tokens[tokens.len()-1]))
    } else { None };

    let dt = if last_two.as_deref() == Some("double precision") {
        DataType::Float8
    } else {
        match last {
            "int" | "int4" | "integer" => DataType::Int4,
            "bigint" | "int8"          => DataType::Int8,
            "float8" | "double"        => DataType::Float8,
            other => return Err(fe(format!("unsupported type: {other}"))),
        }
    };
    Ok(dt)
}
