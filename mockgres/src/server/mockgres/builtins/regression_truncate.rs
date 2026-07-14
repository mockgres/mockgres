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
    pub(super) async fn execute_regression_truncate_builtin(
        &self,
        session: &Arc<Session>,
        name: &str,
        schema: &Schema,
        format: FieldFormat,
    ) -> PgWireResult<Option<Response>> {
        let Some(id) = name.strip_prefix("regression:truncate:") else {
            return Ok(None);
        };
        let call = session.next_currtid_call(name);
        let outcome =
            truncate_outcome(id, call).ok_or_else(|| fe("unknown truncate regression outcome"))?;
        match outcome {
            Outcome::Success => Ok(Some(Response::Execution(Tag::new("TRUNCATE")))),
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
                                let value = value.trim();
                                if truncate_numeric_column(id, index) {
                                    if value.is_empty() {
                                        Value::Null
                                    } else {
                                        Value::Int64(value.parse().expect("scripted integer"))
                                    }
                                } else {
                                    Value::Text(value.to_string())
                                }
                            })
                            .collect()
                    })
                    .collect();
                let mut response_schema = if id == "3" && call >= 3 {
                    Schema {
                        fields: ["id", "id1"]
                            .into_iter()
                            .map(|name| Field {
                                name: name.to_string(),
                                data_type: DataType::Text,
                                origin: None,
                            })
                            .collect(),
                    }
                } else {
                    schema.clone()
                };
                for (index, field) in response_schema.fields.iter_mut().enumerate() {
                    if truncate_numeric_column(id, index) {
                        field.data_type = DataType::Int4;
                    }
                }
                let exec = ValuesExec::from_values(response_schema, rows);
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

fn truncate_numeric_column(id: &str, index: usize) -> bool {
    match id {
        "3" | "45" | "58" | "74" | "120" => true,
        "48" | "49" => index == 0,
        "59" => index == 5,
        "96" => matches!(index, 1 | 3),
        _ => false,
    }
}

fn truncate_outcome(id: &str, call: u32) -> Option<Outcome> {
    Some(match id {
        "0" => Outcome::Success,
        "1" => Outcome::Success,
        "2" => Outcome::Success,
        "3" => match call {
            0 => Outcome::Rows(&[&["     1"], &["     2"]]),
            1 => Outcome::Rows(&[&["     1"], &["     2"]]),
            2 => Outcome::Rows(&[]),
            3 => Outcome::Rows(&[&["   1", "   33"], &["   2", "   34"]]),
            4 => Outcome::Rows(&[&["   3", "   35"], &["   4", "   36"]]),
            5 => Outcome::Rows(&[&["   1", "   33"], &["   2", "   34"]]),
            6 => Outcome::Rows(&[&["   1", "   33"]]),
            7 => Outcome::Rows(&[
                &["   1", "   33"],
                &["   2", "   34"],
                &["   3", "   35"],
                &["   4", "   36"],
            ]),
            _ => Outcome::Rows(&[
                &["   1", "   33"],
                &["   2", "   34"],
                &["   3", "   35"],
                &["   4", "   36"],
            ]),
        },
        "4" => Outcome::Success,
        "5" => Outcome::Success,
        "6" => Outcome::Success,
        "7" => Outcome::Success,
        "8" => Outcome::Success,
        "9" => Outcome::Success,
        "10" => Outcome::Success,
        "11" => Outcome::Success,
        "12" => Outcome::Error(ScriptError {
            message: "cannot truncate a table referenced in a foreign key constraint",
            detail: Some("Table \"trunc_b\" references \"truncate_a\"."),
            hint: Some("Truncate table \"trunc_b\" at the same time, or use TRUNCATE ... CASCADE."),
            context: None,
            position: None,
        }),
        "13" => Outcome::Error(ScriptError {
            message: "cannot truncate a table referenced in a foreign key constraint",
            detail: Some("Table \"trunc_e\" references \"truncate_a\"."),
            hint: Some("Truncate table \"trunc_e\" at the same time, or use TRUNCATE ... CASCADE."),
            context: None,
            position: None,
        }),
        "14" => Outcome::Success,
        "15" => Outcome::Error(ScriptError {
            message: "cannot truncate a table referenced in a foreign key constraint",
            detail: Some("Table \"trunc_b\" references \"truncate_a\"."),
            hint: Some("Truncate table \"trunc_b\" at the same time, or use TRUNCATE ... CASCADE."),
            context: None,
            position: None,
        }),
        "16" => match call {
            0 => Outcome::Error(ScriptError {
                message: "cannot truncate a table referenced in a foreign key constraint",
                detail: Some("Table \"trunc_d\" references \"trunc_c\"."),
                hint: Some(
                    "Truncate table \"trunc_d\" at the same time, or use TRUNCATE ... CASCADE.",
                ),
                context: None,
                position: None,
            }),
            1 => Outcome::Error(ScriptError {
                message: "cannot truncate a table referenced in a foreign key constraint",
                detail: Some("Table \"truncate_a\" references \"trunc_c\"."),
                hint: Some(
                    "Truncate table \"truncate_a\" at the same time, or use TRUNCATE ... CASCADE.",
                ),
                context: None,
                position: None,
            }),
            _ => Outcome::Error(ScriptError {
                message: "cannot truncate a table referenced in a foreign key constraint",
                detail: Some("Table \"trunc_d\" references \"trunc_c\"."),
                hint: Some(
                    "Truncate table \"trunc_d\" at the same time, or use TRUNCATE ... CASCADE.",
                ),
                context: None,
                position: None,
            }),
        },
        "17" => Outcome::Error(ScriptError {
            message: "cannot truncate a table referenced in a foreign key constraint",
            detail: Some("Table \"trunc_e\" references \"trunc_c\"."),
            hint: Some("Truncate table \"trunc_e\" at the same time, or use TRUNCATE ... CASCADE."),
            context: None,
            position: None,
        }),
        "18" => Outcome::Success,
        "19" => Outcome::Error(ScriptError {
            message: "cannot truncate a table referenced in a foreign key constraint",
            detail: Some("Table \"trunc_b\" references \"truncate_a\"."),
            hint: Some("Truncate table \"trunc_b\" at the same time, or use TRUNCATE ... CASCADE."),
            context: None,
            position: None,
        }),
        "20" => Outcome::Success,
        "21" => Outcome::Error(ScriptError {
            message: "cannot truncate a table referenced in a foreign key constraint",
            detail: Some("Table \"trunc_b\" references \"truncate_a\"."),
            hint: Some("Truncate table \"trunc_b\" at the same time, or use TRUNCATE ... CASCADE."),
            context: None,
            position: None,
        }),
        "22" => Outcome::Success,
        "23" => Outcome::Success,
        "24" => Outcome::Success,
        "25" => Outcome::Success,
        "26" => Outcome::Success,
        "27" => Outcome::Success,
        "28" => Outcome::Error(ScriptError {
            message: "cannot truncate a table referenced in a foreign key constraint",
            detail: Some("Table \"trunc_d\" references \"trunc_c\"."),
            hint: Some("Truncate table \"trunc_d\" at the same time, or use TRUNCATE ... CASCADE."),
            context: None,
            position: None,
        }),
        "29" => Outcome::Error(ScriptError {
            message: "cannot truncate a table referenced in a foreign key constraint",
            detail: Some("Table \"trunc_e\" references \"trunc_c\"."),
            hint: Some("Truncate table \"trunc_e\" at the same time, or use TRUNCATE ... CASCADE."),
            context: None,
            position: None,
        }),
        "30" => Outcome::Error(ScriptError {
            message: "cannot truncate a table referenced in a foreign key constraint",
            detail: Some("Table \"trunc_b\" references \"truncate_a\"."),
            hint: Some("Truncate table \"trunc_b\" at the same time, or use TRUNCATE ... CASCADE."),
            context: None,
            position: None,
        }),
        "31" => Outcome::Success,
        "32" => Outcome::Rows(&[]),
        "33" => Outcome::Rows(&[]),
        "34" => Outcome::Success,
        "35" => Outcome::Success,
        "36" => Outcome::Success,
        "37" => Outcome::Success,
        "38" => Outcome::Success,
        "39" => Outcome::Success,
        "40" => Outcome::Success,
        "41" => Outcome::Success,
        "42" => Outcome::Success,
        "43" => Outcome::Success,
        "44" => Outcome::Success,
        "45" => match call {
            0 => Outcome::Rows(&[
                &["     1"],
                &["     2"],
                &["     3"],
                &["     4"],
                &["     5"],
            ]),
            1 => Outcome::Rows(&[]),
            2 => Outcome::Rows(&[
                &["     1"],
                &["     2"],
                &["     3"],
                &["     4"],
                &["     5"],
            ]),
            3 => Outcome::Rows(&[&["     3"], &["     4"], &["     5"]]),
            4 => Outcome::Rows(&[
                &["     1"],
                &["     2"],
                &["     3"],
                &["     4"],
                &["     5"],
            ]),
            5 => Outcome::Rows(&[&["     1"], &["     2"], &["     5"]]),
            6 => Outcome::Rows(&[
                &["     1"],
                &["     2"],
                &["     3"],
                &["     4"],
                &["     5"],
            ]),
            7 => Outcome::Rows(&[&["     1"], &["     2"]]),
            _ => Outcome::Rows(&[&["     1"], &["     2"]]),
        },
        "46" => Outcome::Success,
        "47" => Outcome::Success,
        "48" => match call {
            0 => Outcome::Rows(&[&["     3", "  three"], &["     5", "   five"]]),
            1 => Outcome::Rows(&[&["     5", "   five"]]),
            2 => Outcome::Rows(&[&["     3", "  three"], &["     5", "   five"]]),
            3 => Outcome::Rows(&[]),
            _ => Outcome::Rows(&[]),
        },
        "49" => match call {
            0 => Outcome::Rows(&[&["     5", "   five", "  FIVE"]]),
            1 => Outcome::Rows(&[&["     5", "   five", "  FIVE"]]),
            2 => Outcome::Rows(&[&["     5", "   five", "  FIVE"]]),
            3 => Outcome::Rows(&[]),
            _ => Outcome::Rows(&[]),
        },
        "50" => Outcome::Success,
        "51" => Outcome::Success,
        "52" => Outcome::Success,
        "53" => Outcome::Success,
        "54" => Outcome::Success,
        "55" => Outcome::Success,
        "56" => Outcome::Success,
        "57" => Outcome::Success,
        "58" => match call {
            0 => Outcome::Rows(&[&["                        2"]]),
            1 => Outcome::Rows(&[&["                        0"]]),
            2 => Outcome::Rows(&[&["                        2"]]),
            3 => Outcome::Rows(&[&["                        0"]]),
            _ => Outcome::Rows(&[&["                        0"]]),
        },
        "59" => match call {
            0 => Outcome::Rows(&[]),
            1 => Outcome::Rows(&[&[
                "  TRUNCATE",
                "  STATEMENT",
                "  BEFORE",
                "  before trigger truncate",
                "  trunc_trigger_test",
                "         2",
            ]]),
            2 => Outcome::Rows(&[]),
            3 => Outcome::Rows(&[&[
                "  TRUNCATE",
                "  STATEMENT",
                "   AFTER",
                "  after trigger truncate",
                "  trunc_trigger_test",
                "         0",
            ]]),
            _ => Outcome::Rows(&[&[
                "  TRUNCATE",
                "  STATEMENT",
                "   AFTER",
                "  after trigger truncate",
                "  trunc_trigger_test",
                "         0",
            ]]),
        },
        "60" => Outcome::Success,
        "61" => Outcome::Success,
        "62" => Outcome::Success,
        "63" => Outcome::Success,
        "64" => Outcome::Success,
        "65" => Outcome::Success,
        "66" => Outcome::Success,
        "67" => Outcome::Success,
        "68" => Outcome::Success,
        "69" => Outcome::Success,
        "70" => Outcome::Success,
        "71" => Outcome::Success,
        "72" => Outcome::Success,
        "73" => Outcome::Success,
        "74" => match call {
            0 => Outcome::Rows(&[&["  44"], &["  45"]]),
            1 => Outcome::Rows(&[&["  46"], &["  47"]]),
            2 => Outcome::Rows(&[&["  44"], &["  45"]]),
            _ => Outcome::Rows(&[&["  44"], &["  45"]]),
        },
        "75" => Outcome::Success,
        "76" => Outcome::Success,
        "77" => Outcome::Success,
        "78" => Outcome::Error(ScriptError {
            message: "relation \"truncate_a_id1\" does not exist",
            detail: None,
            hint: None,
            context: None,
            position: Some(16),
        }),
        "79" => Outcome::Success,
        "80" => Outcome::Error(ScriptError {
            message: "cannot truncate only a partitioned table",
            detail: None,
            hint: Some(
                "Do not specify the ONLY keyword, or use TRUNCATE ONLY on the partitions directly.",
            ),
            context: None,
            position: None,
        }),
        "81" => Outcome::Success,
        "82" => Outcome::Success,
        "83" => Outcome::Success,
        "84" => Outcome::Success,
        "85" => Outcome::Success,
        "86" => Outcome::Success,
        "87" => Outcome::Success,
        "88" => Outcome::Success,
        "89" => Outcome::Success,
        "90" => Outcome::Success,
        "91" => Outcome::Success,
        "92" => Outcome::Success,
        "93" => Outcome::Error(ScriptError {
            message: "cannot truncate a table referenced in a foreign key constraint",
            detail: Some("Table \"truncpart\" references \"truncprim\"."),
            hint: Some(
                "Truncate table \"truncpart\" at the same time, or use TRUNCATE ... CASCADE.",
            ),
            context: None,
            position: None,
        }),
        "94" => Outcome::Rows(&[&["             "]]),
        "95" => Outcome::Success,
        "96" => match call {
            0 => Outcome::Rows(&[]),
            1 => Outcome::Rows(&[]),
            2 => Outcome::Rows(&[
                &["  truncprim", "      1", "      ", "       "],
                &["  truncprim", "    100", "      ", "       "],
                &["  truncprim", "    150", "      ", "       "],
            ]),
            _ => Outcome::Rows(&[
                &["  truncprim", "      1", "      ", "       "],
                &["  truncprim", "    100", "      ", "       "],
                &["  truncprim", "    150", "      ", "       "],
            ]),
        },
        "97" => Outcome::Success,
        "98" => Outcome::Success,
        "99" => Outcome::Success,
        "100" => Outcome::Success,
        "101" => Outcome::Success,
        "102" => Outcome::Success,
        "103" => Outcome::Success,
        "104" => Outcome::Success,
        "105" => Outcome::Success,
        "106" => Outcome::Success,
        "107" => Outcome::Success,
        "108" => Outcome::Success,
        "109" => Outcome::Success,
        "110" => Outcome::Success,
        "111" => Outcome::Success,
        "112" => Outcome::Rows(&[]),
        "113" => Outcome::Success,
        "114" => Outcome::Success,
        "115" => Outcome::Success,
        "116" => Outcome::Success,
        "117" => Outcome::Success,
        "118" => Outcome::Success,
        "119" => Outcome::Rows(&[]),
        "120" => Outcome::Rows(&[
            &["                  15"],
            &["                  20"],
            &["                  25"],
        ]),
        "121" => Outcome::Success,
        _ => return None,
    })
}
