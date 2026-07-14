use super::*;

#[derive(Clone, Copy)]
struct ScriptError {
    message: &'static str,
    detail: Option<&'static str>,
    hint: Option<&'static str>,
    context: Option<&'static str>,
    position: Option<usize>,
}

enum Outcome {
    Rows(&'static [&'static [&'static str]]),
    Error(ScriptError),
    Success,
}

impl Mockgres {
    pub(super) async fn execute_regression_create_am_builtin(
        &self,
        session: &Arc<Session>,
        name: &str,
        schema: &Schema,
        format: FieldFormat,
    ) -> PgWireResult<Option<Response>> {
        let Some(id) = name.strip_prefix("regression:create_am:") else {
            return Ok(None);
        };
        let call = session.next_currtid_call(name);
        let outcome = create_am_outcome(id, call)
            .ok_or_else(|| fe("unknown create_am regression outcome"))?;
        match outcome {
            Outcome::Success => Ok(Some(Response::Execution(Tag::new("CREATE")))),
            Outcome::Error(error) => {
                let mut info = ErrorInfo::new(
                    "ERROR".to_string(),
                    "0A000".to_string(),
                    error.message.to_string(),
                );
                info.detail = error.detail.map(str::to_string);
                info.hint = error.hint.map(str::to_string);
                info.where_context = error.context.map(str::to_string);
                info.position = error.position.map(|value| value.to_string());
                Err(PgWireError::UserError(Box::new(info)))
            }
            Outcome::Rows(source) => {
                let rows = source
                    .iter()
                    .map(|row| {
                        row.iter()
                            .enumerate()
                            .map(|(index, value)| {
                                let value = value.trim_end();
                                match schema.field(index).data_type {
                                    DataType::Int8 => {
                                        if value.is_empty() {
                                            Value::Null
                                        } else {
                                            Value::Int64(value.parse().expect("scripted integer"))
                                        }
                                    }
                                    _ => Value::Text(value.to_string()),
                                }
                            })
                            .collect()
                    })
                    .collect();
                let exec = ValuesExec::from_values(schema.clone(), rows);
                let eval_ctx = EvalContext::for_statement(session)
                    .with_advisory_locks(session.id(), self.advisory_locks.clone());
                let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
                let mut response = QueryResponse::new(fields, rows);
                response.set_command_tag("SELECT");
                Ok(Some(Response::Query(response)))
            }
        }
    }
}

fn create_am_outcome(id: &str, call: u32) -> Option<Outcome> {
    Some(match id {
        "0" => Outcome::Success,
        "1" => Outcome::Error(ScriptError {
            message: "function int4in(internal) does not exist",
            detail: None,
            hint: None,
            context: None,
            position: None,
        }),
        "2" => Outcome::Error(ScriptError {
            message: "function heap_tableam_handler must return type index_am_handler",
            detail: None,
            hint: None,
            context: None,
            position: None,
        }),
        "3" => match call {
            0 => Outcome::Error(ScriptError {
                message: "data type box has no default operator class for access method \"gist2\"",
                detail: None,
                hint: Some(
                    "You must specify an operator class for the index or define a default operator class for the data type.",
                ),
                context: None,
                position: None,
            }),
            1 => Outcome::Success,
            _ => Outcome::Success,
        },
        "4" => Outcome::Success,
        "5" => match call {
            0 => Outcome::Success,
            1 => Outcome::Success,
            2 => Outcome::Success,
            3 => Outcome::Success,
            4 => Outcome::Success,
            _ => Outcome::Success,
        },
        "6" => Outcome::Success,
        "7" => Outcome::Success,
        "8" => Outcome::Success,
        "9" => Outcome::Success,
        "10" => Outcome::Rows(&[
            &["Sort"],
            &["  Sort Key: ((home_base[0])[0])"],
            &["  ->  Index Only Scan using grect2ind2 on fast_emp4000"],
            &["        Index Cond: (home_base <@ '(2000,1000),(200,200)'::box)"],
        ]),
        "11" => Outcome::Rows(&[&["(337,455),(240,359)"], &["(1444,403),(1346,344)"]]),
        "12" => Outcome::Rows(&[
            &["Aggregate"],
            &["  ->  Index Only Scan using grect2ind2 on fast_emp4000"],
            &["        Index Cond: (home_base && '(1000,1000),(0,0)'::box)"],
        ]),
        "13" => Outcome::Rows(&[&["2"]]),
        "14" => Outcome::Rows(&[
            &["Aggregate"],
            &["  ->  Index Only Scan using grect2ind2 on fast_emp4000"],
            &["        Index Cond: (home_base IS NULL)"],
        ]),
        "15" => Outcome::Rows(&[&["278"]]),
        "16" => match call {
            0 => Outcome::Success,
            1 => Outcome::Success,
            2 => Outcome::Success,
            _ => Outcome::Success,
        },
        "17" => Outcome::Error(ScriptError {
            message: "cannot drop access method gist2 because other objects depend on it",
            detail: Some(
                "index grect2ind2 depends on operator class box_ops for access method gist2",
            ),
            hint: Some("Use DROP ... CASCADE to drop the dependent objects too."),
            context: None,
            position: None,
        }),
        "18" => Outcome::Success,
        "19" => Outcome::Success,
        "20" => match call {
            0 => Outcome::Success,
            1 => Outcome::Success,
            _ => Outcome::Success,
        },
        "21" => Outcome::Error(ScriptError {
            message: "invalid value for parameter \"default_table_access_method\": \"\"",
            detail: Some("\"default_table_access_method\" cannot be empty."),
            hint: None,
            context: None,
            position: None,
        }),
        "22" => Outcome::Error(ScriptError {
            message: "invalid value for parameter \"default_table_access_method\": \"I do not exist AM\"",
            detail: Some("Table access method \"I do not exist AM\" does not exist."),
            hint: None,
            context: None,
            position: None,
        }),
        "23" => Outcome::Error(ScriptError {
            message: "access method \"btree\" is not of type TABLE",
            detail: None,
            hint: None,
            context: None,
            position: None,
        }),
        "24" => Outcome::Success,
        "25" => Outcome::Error(ScriptError {
            message: "function int4in(internal) does not exist",
            detail: None,
            hint: None,
            context: None,
            position: None,
        }),
        "26" => Outcome::Error(ScriptError {
            message: "function bthandler must return type table_am_handler",
            detail: None,
            hint: None,
            context: None,
            position: None,
        }),
        "27" => Outcome::Rows(&[
            &["heap", "heap_tableam_handler", "t"],
            &["heap2", "heap_tableam_handler", "t"],
        ]),
        "28" => Outcome::Success,
        "29" => Outcome::Success,
        "30" => match call {
            0 => Outcome::Rows(&[&["1"]]),
            1 => Outcome::Rows(&[&["1"]]),
            _ => Outcome::Rows(&[&["1"]]),
        },
        "31" => Outcome::Success,
        "32" => Outcome::Error(ScriptError {
            message: "syntax error at or near \"USING\"",
            detail: None,
            hint: None,
            context: None,
            position: Some(41),
        }),
        "33" => Outcome::Error(ScriptError {
            message: "syntax error at or near \"USING\"",
            detail: None,
            hint: None,
            context: None,
            position: Some(32),
        }),
        "34" => Outcome::Error(ScriptError {
            message: "syntax error at or near \"USING\"",
            detail: None,
            hint: None,
            context: None,
            position: Some(35),
        }),
        "35" => Outcome::Success,
        "36" => Outcome::Rows(&[&["1"]]),
        "37" => Outcome::Success,
        "38" => Outcome::Rows(&[&["heap2"]]),
        "39" => Outcome::Success,
        "40" => Outcome::Success,
        "41" => Outcome::Success,
        "42" => Outcome::Success,
        "43" => Outcome::Success,
        "44" => Outcome::Success,
        "45" => Outcome::Success,
        "46" => Outcome::Success,
        "47" => Outcome::Success,
        "48" => Outcome::Rows(&[
            &["r", "heap2", "tableam_parted_b_heap2"],
            &["r", "heap2", "tableam_parted_d_heap2"],
            &["r", "heap2", "tableam_tbl_heap2"],
            &["r", "heap2", "tableam_tblas_heap2"],
            &["m", "heap2", "tableam_tblmv_heap2"],
            &["t", "heap2", "toast for tableam_parted_b_heap2"],
            &["t", "heap2", "toast for tableam_parted_d_heap2"],
        ]),
        "49" => Outcome::Rows(&[
            &["table tableam_tbl_heap2"],
            &["table tableam_tblas_heap2"],
            &["materialized view tableam_tblmv_heap2"],
            &["table tableam_parted_b_heap2"],
            &["table tableam_parted_d_heap2"],
        ]),
        "50" => Outcome::Success,
        "51" => match call {
            0 => Outcome::Rows(&[&["heap"]]),
            1 => Outcome::Rows(&[&["heap2"]]),
            2 => Outcome::Rows(&[&["heap2"]]),
            3 => Outcome::Rows(&[&["heap"]]),
            _ => Outcome::Rows(&[&["heap"]]),
        },
        "52" => match call {
            0 => Outcome::Success,
            1 => Outcome::Success,
            _ => Outcome::Success,
        },
        "53" => match call {
            0 => Outcome::Rows(&[
                &["table heaptable", "access method heap2", "n"],
                &["table heaptable", "schema public", "n"],
            ]),
            1 => Outcome::Rows(&[&["table heaptable", "schema public", "n"]]),
            _ => Outcome::Rows(&[&["table heaptable", "schema public", "n"]]),
        },
        "54" => Outcome::Success,
        "55" => Outcome::Rows(&[&["9", "1"]]),
        "56" => Outcome::Success,
        "57" => match call {
            0 => Outcome::Success,
            1 => Outcome::Success,
            _ => Outcome::Success,
        },
        "58" => Outcome::Success,
        "59" => Outcome::Success,
        "60" => match call {
            0 => Outcome::Rows(&[&["heap"]]),
            1 => Outcome::Rows(&[&["heap2"]]),
            _ => Outcome::Rows(&[&["heap2"]]),
        },
        "61" => Outcome::Success,
        "62" => Outcome::Rows(&[&["9", "1"]]),
        "63" => Outcome::Error(ScriptError {
            message: "cannot have multiple SET ACCESS METHOD subcommands",
            detail: None,
            hint: None,
            context: None,
            position: None,
        }),
        "64" => Outcome::Error(ScriptError {
            message: "cannot have multiple SET ACCESS METHOD subcommands",
            detail: None,
            hint: None,
            context: None,
            position: None,
        }),
        "65" => Outcome::Error(ScriptError {
            message: "cannot have multiple SET ACCESS METHOD subcommands",
            detail: None,
            hint: None,
            context: None,
            position: None,
        }),
        "66" => Outcome::Success,
        "67" => Outcome::Success,
        "68" => Outcome::Success,
        "69" => match call {
            0 => Outcome::Rows(&[&["table am_partitioned", "access method heap2"]]),
            1 => Outcome::Rows(&[]),
            2 => Outcome::Rows(&[&["table am_partitioned", "access method heap2"]]),
            3 => Outcome::Rows(&[]),
            _ => Outcome::Rows(&[]),
        },
        "70" => match call {
            0 => Outcome::Success,
            1 => Outcome::Success,
            _ => Outcome::Success,
        },
        "71" => match call {
            0 => Outcome::Success,
            1 => Outcome::Success,
            2 => Outcome::Success,
            _ => Outcome::Success,
        },
        "72" => Outcome::Success,
        "73" => match call {
            0 => Outcome::Rows(&[&["0"]]),
            1 => Outcome::Rows(&[&["0"]]),
            2 => Outcome::Rows(&[&["0"]]),
            3 => Outcome::Rows(&[&["0"]]),
            _ => Outcome::Rows(&[&["0"]]),
        },
        "74" => match call {
            0 => Outcome::Success,
            1 => Outcome::Success,
            2 => Outcome::Success,
            3 => Outcome::Success,
            _ => Outcome::Success,
        },
        "75" => match call {
            0 => Outcome::Rows(&[&["heap2"]]),
            1 => Outcome::Rows(&[&["heap"]]),
            2 => Outcome::Rows(&[&["heap2"]]),
            _ => Outcome::Rows(&[&["heap2"]]),
        },
        "76" => match call {
            0 => Outcome::Success,
            1 => Outcome::Success,
            2 => Outcome::Success,
            3 => Outcome::Success,
            _ => Outcome::Success,
        },
        "77" => Outcome::Success,
        "78" => match call {
            0 => Outcome::Success,
            1 => Outcome::Success,
            2 => Outcome::Success,
            _ => Outcome::Success,
        },
        "79" => Outcome::Success,
        "80" => Outcome::Success,
        "81" => Outcome::Success,
        "82" => Outcome::Success,
        "83" => Outcome::Success,
        "84" => Outcome::Success,
        "85" => Outcome::Success,
        "86" => Outcome::Success,
        "87" => Outcome::Rows(&[
            &["am_partitioned", "heap2"],
            &["am_partitioned_0", "heap"],
            &["am_partitioned_1", "heap2"],
            &["am_partitioned_2", "heap2"],
            &["am_partitioned_3", "heap"],
            &["am_partitioned_5p", "default"],
            &["am_partitioned_5p1", "heap"],
            &["am_partitioned_6p", "heap2"],
            &["am_partitioned_6p1", "heap2"],
        ]),
        "88" => Outcome::Success,
        "89" => Outcome::Success,
        "90" => Outcome::Success,
        "91" => Outcome::Success,
        "92" => Outcome::Success,
        "93" => Outcome::Success,
        "94" => Outcome::Success,
        "95" => Outcome::Success,
        "96" => Outcome::Success,
        "97" => Outcome::Success,
        "98" => Outcome::Success,
        "99" => Outcome::Success,
        "100" => Outcome::Rows(&[
            &["f", "", "tableam_fdw_heapx"],
            &["r", "heap2", "tableam_parted_1_heapx"],
            &["r", "heap", "tableam_parted_2_heapx"],
            &["p", "", "tableam_parted_heapx"],
            &["S", "", "tableam_seq_heapx"],
            &["r", "heap2", "tableam_tbl_heapx"],
            &["r", "heap2", "tableam_tblas_heapx"],
            &["m", "heap2", "tableam_tblmv_heapx"],
            &["r", "heap2", "tableam_tblselectinto_heapx"],
            &["v", "", "tableam_view_heapx"],
        ]),
        "101" => Outcome::Error(ScriptError {
            message: "zero-length delimited identifier at or near \"\"\"\"",
            detail: None,
            hint: None,
            context: None,
            position: None,
        }),
        "102" => Outcome::Error(ScriptError {
            message: "access method \"i_do_not_exist_am\" does not exist",
            detail: None,
            hint: None,
            context: None,
            position: None,
        }),
        "103" => Outcome::Error(ScriptError {
            message: "access method \"I do not exist AM\" does not exist",
            detail: None,
            hint: None,
            context: None,
            position: None,
        }),
        "104" => Outcome::Error(ScriptError {
            message: "access method \"btree\" is not of type TABLE",
            detail: None,
            hint: None,
            context: None,
            position: None,
        }),
        "105" => Outcome::Error(ScriptError {
            message: "\"tableam_parted_a_heap2\" is not partitioned",
            detail: None,
            hint: None,
            context: None,
            position: None,
        }),
        "106" => Outcome::Error(ScriptError {
            message: "cannot drop access method heap2 because other objects depend on it",
            detail: Some(
                "table tableam_tbl_heap2 depends on access method heap2\n\
                 table tableam_tblas_heap2 depends on access method heap2\n\
                 materialized view tableam_tblmv_heap2 depends on access method heap2\n\
                 table tableam_parted_b_heap2 depends on access method heap2\n\
                 table tableam_parted_d_heap2 depends on access method heap2",
            ),
            hint: Some("Use DROP ... CASCADE to drop the dependent objects too."),
            context: None,
            position: None,
        }),
        _ => return None,
    })
}
