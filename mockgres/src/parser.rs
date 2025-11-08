use crate::engine::{
    BoolExpr, CmpOp, DataType, Field, InsertSource, ObjName, Plan, ScalarBinaryOp, ScalarExpr,
    ScalarFunc, ScalarUnaryOp, Schema, Selection, SortKey, UpdateSet, Value, fe, fe_code,
};
use pg_query::protobuf::a_const::Val;
use pg_query::protobuf::{AConst, AlterTableType, ConstrType, ObjectType, VariableSetKind};
use pg_query::{NodeEnum, parse};
use std::collections::HashMap;
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
                                (dt, ScalarExpr::Literal(Value::Int64(i)))
                            }
                            Value::Float64Bits(b) => {
                                (DataType::Float8, ScalarExpr::Literal(Value::Float64Bits(b)))
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

                let mut projection_items: Option<Vec<(ScalarExpr, String)>> = None;
                let mut selection = if sel.target_list.len() == 1 {
                    let t = &sel.target_list[0];
                    let node = t.node.as_ref().ok_or_else(|| fe("missing target node"))?;
                    if let NodeEnum::ResTarget(rt) = node {
                        if let Some(NodeEnum::ColumnRef(cr)) =
                            rt.val.as_ref().and_then(|n| n.node.as_ref())
                        {
                            if cr
                                .fields
                                .get(0)
                                .and_then(|f| f.node.as_ref())
                                .map(|n| matches!(n, NodeEnum::AStar(_)))
                                .unwrap_or(false)
                            {
                                Selection::Star
                            } else {
                                let expr_node = rt
                                    .val
                                    .as_ref()
                                    .and_then(|n| n.node.as_ref())
                                    .ok_or_else(|| fe("bad target expr"))?;
                                let expr = parse_scalar_expr(expr_node)?;
                                let mut cols = Vec::new();
                                collect_columns_from_scalar_expr(&expr, &mut cols);
                                let alias = if rt.name.is_empty() {
                                    derive_expr_name(&expr)
                                } else {
                                    rt.name.clone()
                                };
                                projection_items = Some(vec![(expr, alias)]);
                                Selection::Columns(cols)
                            }
                        } else {
                            let expr_node = rt
                                .val
                                .as_ref()
                                .and_then(|n| n.node.as_ref())
                                .ok_or_else(|| fe("bad target expr"))?;
                            let expr = parse_scalar_expr(expr_node)?;
                            let alias = if rt.name.is_empty() {
                                derive_expr_name(&expr)
                            } else {
                                rt.name.clone()
                            };
                            let mut cols = Vec::new();
                            collect_columns_from_scalar_expr(&expr, &mut cols);
                            projection_items = Some(vec![(expr, alias)]);
                            Selection::Columns(cols)
                        }
                    } else {
                        return Err(fe("unexpected target type for single column"));
                    }
                } else {
                    let mut cols = Vec::new();
                    let mut exprs = Vec::new();
                    for t in sel.target_list {
                        let rt = t
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
                        let expr = parse_scalar_expr(expr_node)?;
                        collect_columns_from_scalar_expr(&expr, &mut cols);
                        let alias = if rt.name.is_empty() {
                            derive_expr_name(&expr)
                        } else {
                            rt.name.clone()
                        };
                        exprs.push((expr, alias));
                    }
                    projection_items = Some(exprs);
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
                    let mut needed = Vec::new();
                    collect_columns_from_bool_expr(expr, &mut needed);
                    ensure_columns_present(cols, needed, &mut project_prefix_len);
                }
                let mut order_keys: Option<Vec<SortKey>> = None;
                if !sel.sort_clause.is_empty() {
                    let mut keys = parse_order_clause(&sel.sort_clause)?;
                    if let Some(items) = &projection_items {
                        rewrite_order_keys_for_projection(&mut keys, items);
                    }
                    if let Selection::Columns(cols) = &mut selection {
                        let mut needed = Vec::new();
                        collect_columns_from_order_keys(&keys, &mut needed);
                        ensure_columns_present(cols, needed, &mut project_prefix_len);
                    }
                    order_keys = Some(keys);
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
                if let Some(keys) = order_keys {
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

                if let Some(exprs) = projection_items {
                    let schema = Schema {
                        fields: exprs
                            .iter()
                            .map(|(_, name)| Field {
                                name: name.clone(),
                                data_type: DataType::Text,
                            })
                            .collect(),
                    };
                    plan = Plan::Projection {
                        input: Box::new(plan),
                        exprs,
                        schema,
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
                let cmd = cmd_node.node.ok_or_else(|| fe("bad ALTER TABLE command"))?;
                let NodeEnum::AlterTableCmd(cmd) = cmd else {
                    return Err(fe("bad ALTER TABLE command"));
                };
                let subtype = AlterTableType::try_from(cmd.subtype)
                    .map_err(|_| fe("bad ALTER TABLE type"))?;
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

            Some(NodeEnum::IndexStmt(idx)) => {
                let table_rv = idx.relation.ok_or_else(|| fe("missing index table"))?;
                let table = ObjName {
                    schema: if table_rv.schemaname.is_empty() {
                        None
                    } else {
                        Some(table_rv.schemaname)
                    },
                    name: table_rv.relname,
                };
                if idx.idxname.is_empty() {
                    return Err(fe("index name required"));
                }
                let columns = parse_index_columns(&idx.index_params)?;
                Ok(Plan::CreateIndex {
                    name: idx.idxname,
                    table,
                    columns,
                    if_not_exists: idx.if_not_exists,
                })
            }

            Some(NodeEnum::DropStmt(drop)) => {
                let remove_type =
                    ObjectType::try_from(drop.remove_type).map_err(|_| fe("bad drop type"))?;
                if remove_type != ObjectType::ObjectIndex {
                    return Err(fe("only DROP INDEX supported here"));
                }
                if drop.objects.is_empty() {
                    return Err(fe("DROP INDEX requires names"));
                }
                let mut names = Vec::with_capacity(drop.objects.len());
                for obj in drop.objects {
                    let node = obj.node.ok_or_else(|| fe("bad DROP INDEX name"))?;
                    names.push(parse_obj_name_from_list(&node)?);
                }
                Ok(Plan::DropIndex {
                    indexes: names,
                    if_exists: drop.missing_ok,
                })
            }

            Some(NodeEnum::VariableShowStmt(show)) => {
                let schema = Schema {
                    fields: vec![Field {
                        name: show.name.clone(),
                        data_type: DataType::Text,
                    }],
                };
                Ok(Plan::ShowVariable {
                    name: show.name.to_ascii_lowercase(),
                    schema,
                })
            }

            Some(NodeEnum::VariableSetStmt(set)) => {
                let name_lower = set.name.to_ascii_lowercase();
                if name_lower != "client_min_messages" {
                    return Err(fe_code("0A000", format!("SET {} not supported", set.name)));
                }
                let kind = VariableSetKind::try_from(set.kind).map_err(|_| fe("bad SET kind"))?;
                let value = match kind {
                    VariableSetKind::VarSetValue | VariableSetKind::VarSetCurrent => {
                        Some(parse_set_value(&set.args)?)
                    }
                    VariableSetKind::VarSetDefault
                    | VariableSetKind::VarReset
                    | VariableSetKind::VarResetAll => None,
                    VariableSetKind::VarSetMulti => {
                        return Err(fe("SET MULTI not supported"));
                    }
                    VariableSetKind::Undefined => return Err(fe("bad SET kind")),
                };
                Ok(Plan::SetVariable {
                    name: name_lower,
                    value,
                })
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
                let insert_columns = parse_insert_columns(&ins.cols)?;
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
                        if matches!(n, NodeEnum::SetToDefault(_)) {
                            row.push(InsertSource::Default);
                        } else {
                            let expr = parse_insert_value_expr(&n)?;
                            row.push(InsertSource::Expr(expr));
                        }
                    }
                    all_rows.push(row);
                }
                Ok(Plan::InsertValues {
                    table,
                    columns: insert_columns,
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
        NodeEnum::AExpr(ax) => parse_arithmetic_expr(ax),
        NodeEnum::FuncCall(fc) => parse_function_call(fc),
        NodeEnum::TypeCast(tc) => {
            let inner = tc
                .arg
                .as_ref()
                .and_then(|n| n.node.as_ref())
                .ok_or_else(|| fe("bad type cast"))?;
            parse_scalar_expr(inner)
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

fn parse_arithmetic_expr(
    ax: &pg_query::protobuf::AExpr,
) -> pgwire::error::PgWireResult<ScalarExpr> {
    let op = ax
        .name
        .iter()
        .find_map(|n| {
            n.node.as_ref().and_then(|nn| {
                if let NodeEnum::String(s) = nn {
                    Some(s.sval.clone())
                } else {
                    None
                }
            })
        })
        .ok_or_else(|| fe("missing operator"))?;
    if ax.lexpr.is_none() {
        if op == "-" {
            let rhs = ax
                .rexpr
                .as_ref()
                .and_then(|n| n.node.as_ref())
                .ok_or_else(|| fe("bad unary minus"))?;
            let expr = parse_scalar_expr(rhs)?;
            return Ok(ScalarExpr::UnaryOp {
                op: ScalarUnaryOp::Negate,
                expr: Box::new(expr),
            });
        }
    }
    let lexpr = ax
        .lexpr
        .as_ref()
        .and_then(|n| n.node.as_ref())
        .ok_or_else(|| fe("bad lhs"))?;
    let rexpr = ax
        .rexpr
        .as_ref()
        .and_then(|n| n.node.as_ref())
        .ok_or_else(|| fe("bad rhs"))?;
    let left = parse_scalar_expr(lexpr)?;
    let right = parse_scalar_expr(rexpr)?;
    let bin_op = match op.as_str() {
        "+" => ScalarBinaryOp::Add,
        "-" => ScalarBinaryOp::Sub,
        "*" => ScalarBinaryOp::Mul,
        "/" => ScalarBinaryOp::Div,
        "||" => ScalarBinaryOp::Concat,
        other => return Err(fe(format!("unsupported operator: {other}"))),
    };
    Ok(ScalarExpr::BinaryOp {
        op: bin_op,
        left: Box::new(left),
        right: Box::new(right),
    })
}

fn parse_function_call(
    fc: &pg_query::protobuf::FuncCall,
) -> pgwire::error::PgWireResult<ScalarExpr> {
    let name = fc
        .funcname
        .iter()
        .find_map(|n| {
            n.node.as_ref().and_then(|nn| {
                if let NodeEnum::String(s) = nn {
                    Some(s.sval.to_ascii_lowercase())
                } else {
                    None
                }
            })
        })
        .ok_or_else(|| fe("bad function name"))?;
    let mut args = Vec::new();
    for arg in &fc.args {
        let node = arg
            .node
            .as_ref()
            .ok_or_else(|| fe("bad function argument"))?;
        args.push(parse_scalar_expr(node)?);
    }
    let func = match name.as_str() {
        "coalesce" => ScalarFunc::Coalesce,
        "upper" => ScalarFunc::Upper,
        "lower" => ScalarFunc::Lower,
        "length" | "char_length" => ScalarFunc::Length,
        other => return Err(fe(format!("unsupported function: {other}"))),
    };
    match func {
        ScalarFunc::Coalesce => {
            if args.is_empty() {
                return Err(fe("coalesce requires at least one argument"));
            }
        }
        ScalarFunc::Upper | ScalarFunc::Lower | ScalarFunc::Length => {
            if args.len() != 1 {
                return Err(fe("function expects exactly one argument"));
            }
        }
    }
    Ok(ScalarExpr::Func { func, args })
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

fn parse_insert_columns(
    cols: &[pg_query::Node],
) -> pgwire::error::PgWireResult<Option<Vec<String>>> {
    if cols.is_empty() {
        return Ok(None);
    }
    let mut out = Vec::with_capacity(cols.len());
    for c in cols {
        let node = c.node.as_ref().ok_or_else(|| fe("bad insert column"))?;
        let name = match node {
            NodeEnum::ResTarget(rt) => {
                if !rt.name.is_empty() {
                    rt.name.clone()
                } else {
                    extract_col_name(rt)?
                }
            }
            NodeEnum::String(s) => s.sval.clone(),
            _ => return Err(fe("bad insert column")),
        };
        if out.iter().any(|existing| existing == &name) {
            return Err(fe(format!("duplicate insert column: {name}")));
        }
        out.push(name);
    }
    Ok(Some(out))
}

fn parse_insert_value_expr(node: &NodeEnum) -> pgwire::error::PgWireResult<ScalarExpr> {
    let expr = parse_scalar_expr(node)?;
    sanitize_insert_expr(expr)
}

fn sanitize_insert_expr(expr: ScalarExpr) -> pgwire::error::PgWireResult<ScalarExpr> {
    match expr {
        ScalarExpr::Column(name) => {
            if name.eq_ignore_ascii_case("nan") {
                Ok(ScalarExpr::Literal(Value::from_f64(f64::NAN)))
            } else {
                Err(fe("INSERT expressions cannot reference columns"))
            }
        }
        ScalarExpr::BinaryOp { op, left, right } => Ok(ScalarExpr::BinaryOp {
            op,
            left: Box::new(sanitize_insert_expr(*left)?),
            right: Box::new(sanitize_insert_expr(*right)?),
        }),
        ScalarExpr::UnaryOp { op, expr } => Ok(ScalarExpr::UnaryOp {
            op,
            expr: Box::new(sanitize_insert_expr(*expr)?),
        }),
        ScalarExpr::Func { func, args } => {
            let new_args = args
                .into_iter()
                .map(sanitize_insert_expr)
                .collect::<pgwire::error::PgWireResult<Vec<_>>>()?;
            Ok(ScalarExpr::Func {
                func,
                args: new_args,
            })
        }
        other => Ok(other),
    }
}

fn rewrite_order_keys_for_projection(keys: &mut [SortKey], projection: &[(ScalarExpr, String)]) {
    if projection.is_empty() {
        return;
    }
    let mut alias_map: HashMap<String, ScalarExpr> = HashMap::new();
    for (expr, name) in projection {
        alias_map.insert(name.clone(), expr.clone());
        alias_map.insert(name.to_ascii_lowercase(), expr.clone());
    }
    for key in keys.iter_mut() {
        match key {
            SortKey::ByIndex {
                idx,
                asc,
                nulls_first,
            } => {
                if *idx < projection.len() {
                    let expr = projection[*idx].0.clone();
                    *key = SortKey::Expr {
                        expr,
                        asc: *asc,
                        nulls_first: *nulls_first,
                    };
                }
            }
            SortKey::ByName {
                col,
                asc,
                nulls_first,
            } => {
                let lookup = alias_map
                    .get(col)
                    .or_else(|| alias_map.get(&col.to_ascii_lowercase()));
                if let Some(expr) = lookup {
                    *key = SortKey::Expr {
                        expr: expr.clone(),
                        asc: *asc,
                        nulls_first: *nulls_first,
                    };
                }
            }
            _ => {}
        }
    }
}

fn collect_columns_from_order_keys(keys: &[SortKey], out: &mut Vec<String>) {
    for key in keys {
        match key {
            SortKey::ByName { col, .. } => {
                if !out.iter().any(|c| c == col) {
                    out.push(col.clone());
                }
            }
            SortKey::Expr { expr, .. } => collect_columns_from_scalar_expr(expr, out),
            _ => {}
        }
    }
}

fn ensure_columns_present(
    cols: &mut Vec<String>,
    needed: Vec<String>,
    project_prefix_len: &mut Option<usize>,
) {
    if needed.is_empty() {
        return;
    }
    let requested_len = cols.len();
    for name in needed {
        if !cols.iter().any(|c| c == &name) {
            if project_prefix_len.is_none() {
                *project_prefix_len = Some(requested_len);
            }
            cols.push(name);
        }
    }
}

fn collect_columns_from_scalar_expr(expr: &ScalarExpr, out: &mut Vec<String>) {
    match expr {
        ScalarExpr::Column(name) => {
            if !out.iter().any(|c| c == name) {
                out.push(name.clone());
            }
        }
        ScalarExpr::BinaryOp { left, right, .. } => {
            collect_columns_from_scalar_expr(left, out);
            collect_columns_from_scalar_expr(right, out);
        }
        ScalarExpr::UnaryOp { expr, .. } => collect_columns_from_scalar_expr(expr, out),
        ScalarExpr::Func { args, .. } => {
            for arg in args {
                collect_columns_from_scalar_expr(arg, out);
            }
        }
        ScalarExpr::ColumnIdx(_) | ScalarExpr::Literal(_) | ScalarExpr::Param { .. } => {}
    }
}

fn derive_expr_name(expr: &ScalarExpr) -> String {
    match expr {
        ScalarExpr::Column(name) => name.clone(),
        ScalarExpr::ColumnIdx(_) => "?column?".into(),
        ScalarExpr::Literal(_) => "?column?".into(),
        _ => "?column?".into(),
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
            _ => {
                let expr = parse_scalar_expr(expr)?;
                SortKey::Expr {
                    expr,
                    asc,
                    nulls_first,
                }
            }
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
                    cons.raw_expr.as_ref().and_then(|expr| expr.node.as_ref())
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

fn parse_index_columns(params: &[pg_query::Node]) -> pgwire::error::PgWireResult<Vec<String>> {
    if params.is_empty() {
        return Err(fe("index requires at least one column"));
    }
    let mut cols = Vec::with_capacity(params.len());
    for p in params {
        let node = p.node.as_ref().ok_or_else(|| fe("bad index column"))?;
        let NodeEnum::IndexElem(elem) = node else {
            return Err(fe("index expressions not supported"));
        };
        if elem.expr.is_some() {
            return Err(fe("expression indexes not supported"));
        }
        if elem.name.is_empty() {
            return Err(fe("index column name required"));
        }
        cols.push(elem.name.clone());
    }
    Ok(cols)
}

fn parse_obj_name_from_list(node: &NodeEnum) -> pgwire::error::PgWireResult<ObjName> {
    let NodeEnum::List(list) = node else {
        return Err(fe("qualified name must be a list"));
    };
    if list.items.is_empty() {
        return Err(fe("empty name list"));
    }
    let mut parts = Vec::new();
    for item in &list.items {
        let Some(NodeEnum::String(s)) = item.node.as_ref() else {
            return Err(fe("expected identifier in name"));
        };
        parts.push(s.sval.clone());
    }
    let name = parts.pop().unwrap();
    let schema = if parts.is_empty() {
        None
    } else if parts.len() == 1 {
        Some(parts.remove(0))
    } else {
        return Err(fe("schema-qualified name can only include schema.table"));
    };
    Ok(ObjName { schema, name })
}

fn parse_set_value(args: &[pg_query::Node]) -> pgwire::error::PgWireResult<String> {
    if args.len() != 1 {
        return Err(fe("SET expects a single value"));
    }
    let node = args[0].node.as_ref().ok_or_else(|| fe("bad SET value"))?;
    let NodeEnum::AConst(c) = node else {
        return Err(fe("SET value must be constant"));
    };
    let value = const_to_value(c)?;
    literal_value_to_string(value)
}

fn literal_value_to_string(value: Value) -> pgwire::error::PgWireResult<String> {
    Ok(match value {
        Value::Text(s) => s,
        Value::Int64(i) => i.to_string(),
        Value::Bool(b) => {
            if b {
                "true".into()
            } else {
                "false".into()
            }
        }
        _ => return Err(fe("SET literal type not supported")),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_alter_table_add_column_default() {
        let plan = Planner::plan_sql("alter table items add column note text default 'pending'")
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

    #[test]
    fn insert_values_preserves_default_cells() {
        let plan =
            Planner::plan_sql("insert into things values (DEFAULT, 1)").expect("plan insert");
        match plan {
            Plan::InsertValues { columns, rows, .. } => {
                assert!(columns.is_none());
                assert_eq!(rows.len(), 1);
                assert!(matches!(rows[0][0], InsertSource::Default));
            }
            other => panic!("unexpected plan: {other:?}"),
        }
    }

    #[test]
    fn insert_column_list_and_expressions_parse() {
        let plan =
            Planner::plan_sql("insert into gadgets (id, qty, note) values (1, 2 + 3, upper('hi'))")
                .expect("plan insert");
        match plan {
            Plan::InsertValues { columns, rows, .. } => {
                let cols = columns.expect("columns");
                assert_eq!(cols, vec!["id", "qty", "note"]);
                assert_eq!(rows.len(), 1);
                assert!(matches!(rows[0][2], InsertSource::Expr(_)));
            }
            other => panic!("unexpected plan: {other:?}"),
        }
    }

    #[test]
    fn create_and_drop_index_parse() {
        let create = Planner::plan_sql("create index idx_things on items (id, qty)")
            .expect("plan create index");
        match create {
            Plan::CreateIndex {
                name,
                table,
                columns,
                if_not_exists,
            } => {
                assert_eq!(name, "idx_things");
                assert_eq!(table.name, "items");
                assert_eq!(columns, vec!["id".to_string(), "qty".to_string()]);
                assert!(!if_not_exists);
            }
            other => panic!("unexpected plan: {other:?}"),
        }

        let drop =
            Planner::plan_sql("drop index if exists public.idx_things").expect("plan drop index");
        match drop {
            Plan::DropIndex {
                indexes, if_exists, ..
            } => {
                assert!(if_exists);
                assert_eq!(indexes.len(), 1);
                assert_eq!(indexes[0].schema.as_deref(), Some("public"));
                assert_eq!(indexes[0].name, "idx_things");
            }
            other => panic!("unexpected plan: {other:?}"),
        }
    }

    #[test]
    fn show_server_version_parses() {
        let plan = Planner::plan_sql("show server_version").expect("plan show");
        match plan {
            Plan::ShowVariable { name, schema } => {
                assert_eq!(name, "server_version");
                assert_eq!(schema.fields.len(), 1);
                assert_eq!(schema.fields[0].name, "server_version");
            }
            other => panic!("unexpected plan: {other:?}"),
        }
    }

    #[test]
    fn set_client_min_messages_parses() {
        let plan = Planner::plan_sql("set client_min_messages = warning").expect("plan set");
        match plan {
            Plan::SetVariable { name, value } => {
                assert_eq!(name, "client_min_messages");
                assert_eq!(value.as_deref(), Some("warning"));
            }
            other => panic!("unexpected plan: {other:?}"),
        }
    }
}
