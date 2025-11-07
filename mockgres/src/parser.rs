use crate::engine::{
    BoolExpr, CmpOp, DataType, Expr, Field, InsertSource, ObjName, Plan, ScalarExpr, Schema,
    Selection, SortKey, UpdateSet, Value, fe,
};
use pg_query::protobuf::a_const::Val;
use pg_query::protobuf::{AConst, AlterTableType, ConstrType};
use pg_query::{NodeEnum, parse};
use std::convert::TryFrom;

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
                            Value::Text(_)
                            | Value::Bool(_)
                            | Value::Date(_)
                            | Value::TimestampMicros(_)
                            | Value::Bytes(_) => {
                                return Err(fe("only numeric literals supported here"));
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
                        let node_enum = t
                            .node
                            .as_ref()
                            .map(|n| &*n)
                            .ok_or_else(|| fe("bad target"))?;
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
                let where_expr =
                    if let Some(w) = sel.where_clause.as_ref().and_then(|n| n.node.as_ref()) {
                        Some(parse_bool_expr(w)?)
                    } else {
                        None
                    };

                // if WHERE references columns not in the projection, add them and remember to
                // project them away post-filter to keep user-visible schema intact.
                let mut project_prefix_len: Option<usize> = None;
                if let (Selection::Columns(cols), Some(expr)) = (&mut selection, &where_expr) {
                    let requested_len = cols.len();
                    let mut needed = Vec::new();
                    collect_columns_from_bool_expr(expr, &mut needed);
                    for name in needed {
                        if !cols.iter().any(|c| c == &name) {
                            if project_prefix_len.is_none() {
                                project_prefix_len = Some(requested_len);
                            }
                            cols.push(name);
                        }
                    }
                }

                // start with unbound scan, then wrap in filter → order → limit
                let mut plan = Plan::UnboundSeqScan { table, selection };

                // where
                if let Some(pred) = where_expr {
                    plan = Plan::Filter {
                        input: Box::new(plan),
                        expr: pred,
                        project_prefix_len,
                    };
                }

                // order by: ordinals or column names
                if !sel.sort_clause.is_empty() {
                    let keys = parse_order_clause(&sel.sort_clause)?;
                    plan = Plan::Order {
                        input: Box::new(plan),
                        keys,
                    };
                }

                // limit: integer constant
                if let Some(limit_node) = sel.limit_count.as_ref().and_then(|n| n.node.as_ref()) {
                    let lim = parse_limit_count(limit_node)?;
                    plan = Plan::Limit {
                        input: Box::new(plan),
                        limit: lim,
                    };
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
                            let (cname, dt, nullable, default) = parse_column_def(&cd)?;
                            let col_name_clone = cname.clone();
                            cols.push((cname, dt, nullable, default));
                            // simple “col PRIMARY KEY” handling
                            if cd.constraints.iter().any(|c| {
                                matches!(
                                    c.node.as_ref(),
                                    Some(NodeEnum::Constraint(cons))
                                        if cons.contype
                                            == ConstrType::ConstrPrimary as i32
                                )
                            }) {
                                pk = Some(vec![col_name_clone]);
                            }
                        }
                        NodeEnum::Constraint(cons) => {
                            // PRIMARY KEY (a,b)
                            if cons.contype == ConstrType::ConstrPrimary as i32 {
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

            Some(NodeEnum::AlterTableStmt(at)) => {
                let rv = at.relation.ok_or_else(|| fe("missing table name"))?;
                let table = ObjName {
                    schema: if rv.schemaname.is_empty() {
                        None
                    } else {
                        Some(rv.schemaname)
                    },
                    name: rv.relname,
                };
                if at.cmds.len() != 1 {
                    return Err(fe("one ALTER TABLE command at a time"));
                }
                let cmd_node = at.cmds.into_iter().next().unwrap();
                let cmd = cmd_node
                    .node
                    .ok_or_else(|| fe("bad ALTER TABLE command"))?;
                let NodeEnum::AlterTableCmd(cmd) = cmd else {
                    return Err(fe("bad ALTER TABLE command"));
                };
                let subtype =
                    AlterTableType::try_from(cmd.subtype).map_err(|_| fe("bad ALTER TABLE type"))?;
                match subtype {
                    AlterTableType::AtAddColumn => {
                        let col_node = cmd
                            .def
                            .as_ref()
                            .and_then(|n| n.node.as_ref())
                            .ok_or_else(|| fe("ADD COLUMN requires column definition"))?;
                        let NodeEnum::ColumnDef(cd) = col_node else {
                            return Err(fe("ADD COLUMN expects column definition"));
                        };
                        let column = parse_column_def(cd)?;
                        Ok(Plan::AlterTableAddColumn {
                            table,
                            column,
                            if_not_exists: cmd.missing_ok,
                        })
                    }
                    AlterTableType::AtDropColumn => {
                        if cmd.name.is_empty() {
                            return Err(fe("DROP COLUMN requires name"));
                        }
                        Ok(Plan::AlterTableDropColumn {
                            table,
                            column: cmd.name,
                            if_exists: cmd.missing_ok,
                        })
                    }
                    _ => Err(fe("unsupported ALTER TABLE command")),
                }
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
                let mut all_rows: Vec<Vec<InsertSource>> = Vec::new();
                for v in sel2.values_lists {
                    let NodeEnum::List(vlist) = v.node.unwrap() else {
                        continue;
                    };
                    let mut row = Vec::new();
                    for cell in vlist.items {
                        let n = cell.node.unwrap();
                        match n {
                            NodeEnum::AConst(c) => {
                                row.push(InsertSource::Expr(Expr::Literal(const_to_value(&c)?)))
                            }
                            NodeEnum::ColumnRef(cr) => {
                                let name = last_colref_name(&cr)?;
                                if name.eq_ignore_ascii_case("nan") {
                                    row.push(InsertSource::Expr(Expr::Literal(Value::from_f64(
                                        f64::NAN,
                                    ))))
                                } else {
                                    return Err(fe("INSERT supports constants only"));
                                }
                            }
                            NodeEnum::SetToDefault(_) => row.push(InsertSource::Default),
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

            Some(NodeEnum::UpdateStmt(upd)) => {
                let rv = upd.relation.ok_or_else(|| fe("missing target table"))?;
                let table = ObjName {
                    schema: if rv.schemaname.is_empty() {
                        None
                    } else {
                        Some(rv.schemaname)
                    },
                    name: rv.relname,
                };

                let mut sets = Vec::new();
                for tgt in upd.target_list {
                    let NodeEnum::ResTarget(rt) = tgt.node.unwrap() else {
                        return Err(fe("bad update target"));
                    };
                    let col_name = if !rt.name.is_empty() {
                        rt.name.clone()
                    } else {
                        extract_col_name(&rt)?
                    };
                    let expr_node = rt
                        .val
                        .as_ref()
                        .and_then(|n| n.node.as_ref())
                        .ok_or_else(|| fe("missing update value"))?;
                    let expr = parse_scalar_expr(expr_node)?;
                    sets.push(UpdateSet::ByName(col_name, expr));
                }
                if sets.is_empty() {
                    return Err(fe("UPDATE requires SET clauses"));
                }

                let filter =
                    if let Some(w) = upd.where_clause.as_ref().and_then(|n| n.node.as_ref()) {
                        Some(parse_bool_expr(w)?)
                    } else {
                        None
                    };

                Ok(Plan::Update {
                    table,
                    sets,
                    filter,
                })
            }

            Some(NodeEnum::DeleteStmt(del)) => {
                let rv = del.relation.ok_or_else(|| fe("missing target table"))?;
                let table = ObjName {
                    schema: if rv.schemaname.is_empty() {
                        None
                    } else {
                        Some(rv.schemaname)
                    },
                    name: rv.relname,
                };
                let filter =
                    if let Some(w) = del.where_clause.as_ref().and_then(|n| n.node.as_ref()) {
                        Some(parse_bool_expr(w)?)
                    } else {
                        None
                    };
                Ok(Plan::Delete { table, filter })
            }

            _ => Err(fe("unsupported statement")),
        }
    }
}

fn parse_bool_expr(node: &NodeEnum) -> pgwire::error::PgWireResult<BoolExpr> {
    use pg_query::protobuf::{BoolExprType, NullTestType};
    match node {
        NodeEnum::BoolExpr(be) => {
            let op = BoolExprType::try_from(be.boolop).map_err(|_| fe("bad bool expr op"))?;
            let mut args = Vec::new();
            for a in &be.args {
                let n = a.node.as_ref().ok_or_else(|| fe("bad bool arg"))?;
                args.push(parse_bool_expr(n)?);
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
            if ax.name.is_empty() {
                if let Some(inner) = ax.lexpr.as_ref().and_then(|n| n.node.as_ref()) {
                    return parse_bool_expr(inner);
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
            let lhs = parse_scalar_expr(lexpr)?;
            let rhs = parse_scalar_expr(rexpr)?;
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
            let expr = parse_scalar_expr(arg)?;
            Ok(BoolExpr::IsNull {
                expr,
                negated: matches!(nt_type, NullTestType::IsNotNull),
            })
        }
        NodeEnum::AConst(c) => match const_to_value(c)? {
            Value::Bool(b) => Ok(BoolExpr::Literal(b)),
            _ => Err(fe("boolean literal expected")),
        },
        NodeEnum::ColumnRef(_) => {
            let col = parse_scalar_expr(node)?;
            Ok(BoolExpr::Comparison {
                lhs: col,
                op: CmpOp::Eq,
                rhs: ScalarExpr::Literal(Value::Bool(true)),
            })
        }
        _ => Err(fe("unsupported WHERE expression")),
    }
}

fn parse_cmp_op(nodes: &[pg_query::protobuf::Node]) -> pgwire::error::PgWireResult<CmpOp> {
    for n in nodes {
        if let Some(NodeEnum::String(s)) = n.node.as_ref() {
            return Ok(match s.sval.as_str() {
                "=" => CmpOp::Eq,
                "!=" | "<>" => CmpOp::Neq,
                "<" => CmpOp::Lt,
                "<=" => CmpOp::Lte,
                ">" => CmpOp::Gt,
                ">=" => CmpOp::Gte,
                other => return Err(fe(format!("unsupported where operator: {other}"))),
            });
        }
    }
    Err(fe("missing operator"))
}

fn parse_scalar_expr(node: &NodeEnum) -> pgwire::error::PgWireResult<ScalarExpr> {
    match node {
        NodeEnum::ColumnRef(cr) => Ok(ScalarExpr::Column(last_colref_name(cr)?)),
        NodeEnum::ParamRef(pr) => {
            if pr.number <= 0 {
                return Err(fe("parameter numbers start at 1"));
            }
            Ok(ScalarExpr::Param {
                idx: (pr.number as usize) - 1,
                ty: None,
            })
        }
        _ => {
            if let Some(v) = try_parse_literal(node)? {
                Ok(ScalarExpr::Literal(v))
            } else {
                Err(fe("unsupported scalar expression"))
            }
        }
    }
}

fn try_parse_literal(node: &NodeEnum) -> pgwire::error::PgWireResult<Option<Value>> {
    match node {
        NodeEnum::AConst(c) => Ok(Some(const_to_value(c)?)),
        NodeEnum::AExpr(ax) => {
            let is_minus = ax.name.iter().any(|nn| {
                matches!(
                    nn.node.as_ref(),
                    Some(NodeEnum::String(s)) if s.sval == "-"
                )
            });
            if is_minus {
                let rhs = ax
                    .rexpr
                    .as_ref()
                    .and_then(|n| n.node.as_ref())
                    .ok_or_else(|| fe("bad unary minus"))?;
                match rhs {
                    NodeEnum::AConst(c) => match const_to_value(c)? {
                        Value::Int64(i) => Ok(Some(Value::Int64(-i))),
                        Value::Float64Bits(b) => Ok(Some(Value::from_f64(-f64::from_bits(b)))),
                        Value::Null => Err(fe("minus over null")),
                        Value::Text(_)
                        | Value::Bool(_)
                        | Value::Date(_)
                        | Value::TimestampMicros(_)
                        | Value::Bytes(_) => Err(fe("minus over non-numeric literal")),
                    },
                    _ => Err(fe("minus over non-const")),
                }
            } else {
                Ok(None)
            }
        }
        _ => Ok(None),
    }
}

fn collect_columns_from_bool_expr(expr: &BoolExpr, out: &mut Vec<String>) {
    match expr {
        BoolExpr::Literal(_) => {}
        BoolExpr::Comparison { lhs, rhs, .. } => {
            collect_columns_from_scalar_expr(lhs, out);
            collect_columns_from_scalar_expr(rhs, out);
        }
        BoolExpr::And(parts) | BoolExpr::Or(parts) => {
            for p in parts {
                collect_columns_from_bool_expr(p, out);
            }
        }
        BoolExpr::Not(inner) => collect_columns_from_bool_expr(inner, out),
        BoolExpr::IsNull { expr, .. } => collect_columns_from_scalar_expr(expr, out),
    }
}

fn collect_columns_from_scalar_expr(expr: &ScalarExpr, out: &mut Vec<String>) {
    if let ScalarExpr::Column(name) = expr {
        if !out.iter().any(|c| c == name) {
            out.push(name.clone());
        }
    }
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

fn parse_order_clause(
    items: &Vec<pg_query::protobuf::Node>,
) -> pgwire::error::PgWireResult<Vec<SortKey>> {
    let mut keys = Vec::new();
    for sb in items {
        let Some(NodeEnum::SortBy(s)) = sb.node.as_ref() else {
            return Err(fe("bad order by"));
        };

        // matching on the SortByDir enum https://github.com/postgres/postgres/blob/master/src/include/nodes/parsenodes.h
        // pg_query shifts enum values up by 1, so 1 is default, 2 is asc, 3 is desc
        let asc = match s.sortby_dir {
            1 | 2 => true,
            3 => false,
            _ => true,
        };

        // parse nulls policy: 1=default, 2=first, 3=last
        let nulls_first = match s.sortby_nulls {
            2 => Some(true),
            3 => Some(false),
            _ => None, // default resolved later: asc => last, desc => first
        };

        let Some(expr) = s.node.as_ref().and_then(|n| n.node.as_ref()) else {
            return Err(fe("bad order by expr"));
        };
        let key = match expr {
            // order by 1
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
            // order by column
            NodeEnum::ColumnRef(cr) => {
                let name = last_colref_name(cr)?;
                SortKey::ByName {
                    col: name,
                    asc,
                    nulls_first,
                }
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
                if iv.ival < 0 {
                    return Err(fe("limit must be non-negative"));
                }
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
                Value::Text(_)
                | Value::Bool(_)
                | Value::Date(_)
                | Value::TimestampMicros(_)
                | Value::Bytes(_) => Err(fe("numeric const expected")),
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
                    Value::Text(_)
                    | Value::Bool(_)
                    | Value::Date(_)
                    | Value::TimestampMicros(_)
                    | Value::Bytes(_) => Err(fe("numeric const expected")),
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
        Val::Boolval(b) => Ok(Value::Bool(b.boolval)),
        Val::Sval(s) => Ok(Value::Text(s.sval.clone())),
        Val::Bsval(_) => Err(fe("bitstring const not yet supported")),
    }
}

fn map_type(cd: &pg_query::protobuf::ColumnDef) -> pgwire::error::PgWireResult<DataType> {
    // subset: int/int4/integer, bigint/int8, float8/double precision
    let typ = cd.type_name.as_ref().ok_or_else(|| fe("missing type"))?;

    // collect tokens, keeping only string components, lowercased
    let mut tokens: Vec<String> = typ
        .names
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
        .collect();

    tokens.retain(|t| t != "pg_catalog" && t != "public");

    if tokens.is_empty() {
        return Err(fe("bad type name"));
    }

    let last = tokens.last().unwrap().as_str();
    let dt = if tokens.len() >= 2
        && tokens[tokens.len() - 2] == "double"
        && tokens[tokens.len() - 1] == "precision"
    {
        DataType::Float8
    } else if tokens.len() >= 4
        && tokens[tokens.len() - 4] == "timestamp"
        && tokens[tokens.len() - 3] == "without"
        && tokens[tokens.len() - 2] == "time"
        && tokens[tokens.len() - 1] == "zone"
    {
        DataType::Timestamp
    } else {
        match last {
            "int" | "int4" | "integer" => DataType::Int4,
            "bigint" | "int8" => DataType::Int8,
            "float8" | "double" => DataType::Float8,
            "text" | "varchar" => DataType::Text,
            "bool" | "boolean" => DataType::Bool,
            "date" => DataType::Date,
            "timestamp" => DataType::Timestamp,
            "bytea" => DataType::Bytea,
            other => return Err(fe(format!("unsupported type: {other}"))),
        }
    };
    Ok(dt)
}

fn parse_column_def(
    cd: &pg_query::protobuf::ColumnDef,
) -> pgwire::error::PgWireResult<(String, DataType, bool, Option<Value>)> {
    let dt = map_type(cd)?;
    let default_node = cd
        .raw_default
        .as_ref()
        .and_then(|n| n.node.as_ref())
        .or_else(|| cd.cooked_default.as_ref().and_then(|n| n.node.as_ref()))
        .or_else(|| {
            cd.constraints.iter().find_map(|c| {
                let Some(NodeEnum::Constraint(cons)) = c.node.as_ref() else {
                    return None;
                };
                if cons.contype == ConstrType::ConstrDefault as i32 {
                    cons.raw_expr
                        .as_ref()
                        .and_then(|expr| expr.node.as_ref())
                } else {
                    None
                }
            })
        });
    let default = if let Some(def) = default_node {
        Some(parse_default_literal(def)?)
    } else {
        None
    };
    Ok((cd.colname.clone(), dt, !cd.is_not_null, default))
}

fn parse_default_literal(node: &NodeEnum) -> pgwire::error::PgWireResult<Value> {
    match node {
        NodeEnum::AConst(c) => const_to_value(c),
        NodeEnum::TypeCast(tc) => {
            let inner = tc
                .arg
                .as_ref()
                .and_then(|n| n.node.as_ref())
                .ok_or_else(|| fe("bad default expression"))?;
            parse_default_literal(inner)
        }
        _ => Err(fe("default must be a constant expression")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_alter_table_add_column_default() {
        let plan = Planner::plan_sql(
            "alter table items add column note text default 'pending'",
        )
        .expect("plan sql");
        match plan {
            Plan::AlterTableAddColumn { column, .. } => {
                let (name, _ty, _nullable, default) = column;
                assert_eq!(name, "note");
                match default {
                    Some(Value::Text(s)) => assert_eq!(s, "pending"),
                    other => panic!("expected text default, got {other:?}"),
                }
            }
            other => panic!("unexpected plan: {other:?}"),
        }
    }
}
