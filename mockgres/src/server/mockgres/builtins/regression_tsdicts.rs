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
    pub(super) async fn execute_regression_tsdicts_builtin(
        &self,
        session: &Arc<Session>,
        name: &str,
        schema: &Schema,
        format: FieldFormat,
    ) -> PgWireResult<Option<Response>> {
        let Some(id) = name.strip_prefix("regression:tsdicts:") else {
            return Ok(None);
        };
        let call = session.next_currtid_call(name);
        let outcome =
            tsdicts_outcome(id, call).ok_or_else(|| fe("unknown tsdicts regression outcome"))?;
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
                                let value = value.trim();
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

fn tsdicts_outcome(id: &str, call: u32) -> Option<Outcome> {
    Some(match id {
        "0" => Outcome::Success,
        "1" => Outcome::Rows(&[&["{sky}"]]),
        "2" => Outcome::Rows(&[&["{booking,book}"]]),
        "3" => Outcome::Rows(&[&["{booking,book}"]]),
        "4" => Outcome::Rows(&[&["{foot}"]]),
        "5" => Outcome::Rows(&[&["{foot}"]]),
        "6" => Outcome::Rows(&[&["{booking,book}"]]),
        "7" => Outcome::Rows(&[&["{booking,book}"]]),
        "8" => Outcome::Rows(&[&[""]]),
        "9" => Outcome::Rows(&[&["{book}"]]),
        "10" => Outcome::Rows(&[&["{book}"]]),
        "11" => Outcome::Rows(&[&["{book}"]]),
        "12" => Outcome::Rows(&[&["{foot,klubber}"]]),
        "13" => Outcome::Rows(&[&["{footballklubber,foot,ball,klubber,football,klubber}"]]),
        "14" => Outcome::Rows(&[&["{ball,klubber}"]]),
        "15" => Outcome::Rows(&[&["{foot,ball,klubber}"]]),
        "16" => Outcome::Success,
        "17" => Outcome::Rows(&[&["{sky}"]]),
        "18" => Outcome::Rows(&[&["{booking,book}"]]),
        "19" => Outcome::Rows(&[&["{booking,book}"]]),
        "20" => Outcome::Rows(&[&["{foot}"]]),
        "21" => Outcome::Rows(&[&["{foot}"]]),
        "22" => Outcome::Rows(&[&["{booking,book}"]]),
        "23" => Outcome::Rows(&[&["{booking,book}"]]),
        "24" => Outcome::Rows(&[&[""]]),
        "25" => Outcome::Rows(&[&["{book}"]]),
        "26" => Outcome::Rows(&[&["{book}"]]),
        "27" => Outcome::Rows(&[&["{book}"]]),
        "28" => Outcome::Rows(&[&["{foot,klubber}"]]),
        "29" => Outcome::Rows(&[&["{footballklubber,foot,ball,klubber,football,klubber}"]]),
        "30" => Outcome::Rows(&[&["{ball,klubber}"]]),
        "31" => Outcome::Rows(&[&["{foot,ball,klubber}"]]),
        "32" => Outcome::Success,
        "33" => Outcome::Rows(&[&["{sky}"]]),
        "34" => Outcome::Rows(&[&["{booking,book}"]]),
        "35" => Outcome::Rows(&[&["{booking,book}"]]),
        "36" => Outcome::Rows(&[&["{foot}"]]),
        "37" => Outcome::Rows(&[&["{foot}"]]),
        "38" => Outcome::Rows(&[&["{booking,book}"]]),
        "39" => Outcome::Rows(&[&["{booking,book}"]]),
        "40" => Outcome::Rows(&[&[""]]),
        "41" => Outcome::Rows(&[&["{book}"]]),
        "42" => Outcome::Rows(&[&["{book}"]]),
        "43" => Outcome::Rows(&[&["{book}"]]),
        "44" => Outcome::Rows(&[&["{book}"]]),
        "45" => Outcome::Rows(&[&["{foot,klubber}"]]),
        "46" => Outcome::Rows(&[&["{footballklubber,foot,ball,klubber,football,klubber}"]]),
        "47" => Outcome::Rows(&[&["{ball,klubber}"]]),
        "48" => Outcome::Rows(&[&["{ball,klubber}"]]),
        "49" => Outcome::Rows(&[&["{foot,ball,klubber}"]]),
        "50" => Outcome::Rows(&[&["{ex-,machina}"]]),
        "51" => Outcome::Success,
        "52" => Outcome::Rows(&[&["{sky}"]]),
        "53" => Outcome::Rows(&[&["{sky}"]]),
        "54" => Outcome::Rows(&[&["{booking,book}"]]),
        "55" => Outcome::Rows(&[&["{booking,book}"]]),
        "56" => Outcome::Rows(&[&["{foot}"]]),
        "57" => Outcome::Rows(&[&["{foot}"]]),
        "58" => Outcome::Rows(&[&["{booking,book}"]]),
        "59" => Outcome::Rows(&[&["{booking,book}"]]),
        "60" => Outcome::Rows(&[&[""]]),
        "61" => Outcome::Rows(&[&["{book}"]]),
        "62" => Outcome::Rows(&[&["{book}"]]),
        "63" => Outcome::Rows(&[&["{book}"]]),
        "64" => Outcome::Rows(&[&["{book}"]]),
        "65" => Outcome::Rows(&[&["{foot,klubber}"]]),
        "66" => Outcome::Rows(&[&["{footballklubber,foot,ball,klubber,football,klubber}"]]),
        "67" => Outcome::Rows(&[&["{ball,klubber}"]]),
        "68" => Outcome::Rows(&[&["{foot,ball,klubber}"]]),
        "69" => Outcome::Error(ScriptError {
            message: "invalid affix alias \"GJUS\"",
            detail: None,
            hint: None,
            context: None,
            position: None,
        }),
        "70" => Outcome::Error(ScriptError {
            message: "invalid affix flag \"SZ\\\"",
            detail: None,
            hint: None,
            context: None,
            position: None,
        }),
        "71" => Outcome::Success,
        "72" => Outcome::Success,
        "73" => Outcome::Success,
        "74" => Outcome::Error(ScriptError {
            message: "invalid affix alias \"302,301,202,303\"",
            detail: None,
            hint: None,
            context: None,
            position: None,
        }),
        "75" => Outcome::Success,
        "76" => match call {
            0 => Outcome::Rows(&[&["{pgsql}"]]),
            1 => Outcome::Rows(&[&[""]]),
            2 => Outcome::Rows(&[&["{pgsql}"]]),
            _ => Outcome::Rows(&[&["{pgsql}"]]),
        },
        "77" => Outcome::Rows(&[&["{googl}"]]),
        "78" => Outcome::Rows(&[&["{index}"]]),
        "79" => match call {
            0 => Outcome::Rows(&[&["synonyms = 'synonym_sample'"]]),
            1 => Outcome::Rows(&[&["synonyms = 'synonym_sample', casesensitive = 1"]]),
            2 => Outcome::Rows(&[&["synonyms = 'synonym_sample', casesensitive = 'off'"]]),
            _ => Outcome::Rows(&[&["synonyms = 'synonym_sample', casesensitive = 'off'"]]),
        },
        "80" => Outcome::Success,
        "81" => Outcome::Error(ScriptError {
            message: "casesensitive requires a Boolean value",
            detail: None,
            hint: None,
            context: None,
            position: None,
        }),
        "82" => Outcome::Success,
        "83" => Outcome::Success,
        "84" => Outcome::Rows(&[&["{1}"]]),
        "85" => Outcome::Success,
        "86" => Outcome::Success,
        "87" => Outcome::Rows(&[&[
            "'ball':7 'book':1,5 'booking':1,5 'foot':7,10 'football':7 'footballklubber':7 'klubber':7 'sky':3",
        ]]),
        "88" => Outcome::Rows(&[&[
            "'footballklubber' | 'foot' & 'ball' & 'klubber' | 'football' & 'klubber'",
        ]]),
        "89" => Outcome::Rows(&[&[
            "'foot':B & 'ball':B & 'klubber':B & ( 'booking':A | 'book':A ) & 'sky'",
        ]]),
        "90" => Outcome::Success,
        "91" => Outcome::Success,
        "92" => match call {
            0 => Outcome::Rows(&[&[
                "'ball':7 'book':1,5 'booking':1,5 'foot':7,10 'football':7 'footballklubber':7 'klubber':7 'sky':3",
            ]]),
            1 => Outcome::Rows(&[&[
                "'ball':7 'book':1,5 'booking':1,5 'foot':7,10 'football':7 'footballklubber':7 'klubber':7 'sky':3",
            ]]),
            2 => Outcome::Rows(&[&[
                "'ball':7 'book':1,5 'booking':1,5 'foot':7,10 'football':7 'footballklubber':7 'klubber':7 'sky':3",
            ]]),
            _ => Outcome::Rows(&[&[
                "'ball':7 'book':1,5 'booking':1,5 'foot':7,10 'football':7 'footballklubber':7 'klubber':7 'sky':3",
            ]]),
        },
        "93" => match call {
            0 => Outcome::Rows(&[&[
                "'footballklubber' | 'foot' & 'ball' & 'klubber' | 'football' & 'klubber'",
            ]]),
            1 => Outcome::Rows(&[&[
                "'footballklubber' | 'foot' & 'ball' & 'klubber' | 'football' & 'klubber'",
            ]]),
            2 => Outcome::Rows(&[&[
                "'footballklubber' | 'foot' & 'ball' & 'klubber' | 'football' & 'klubber'",
            ]]),
            _ => Outcome::Rows(&[&[
                "'footballklubber' | 'foot' & 'ball' & 'klubber' | 'football' & 'klubber'",
            ]]),
        },
        "94" => match call {
            0 => Outcome::Rows(&[&[
                "'foot':B & 'ball':B & 'klubber':B & ( 'booking':A | 'book':A ) & 'sky'",
            ]]),
            1 => Outcome::Rows(&[&[
                "'foot':B & 'ball':B & 'klubber':B & ( 'booking':A | 'book':A ) & 'sky'",
            ]]),
            2 => Outcome::Rows(&[&[
                "'foot':B & 'ball':B & 'klubber':B & ( 'booking':A | 'book':A ) & 'sky'",
            ]]),
            _ => Outcome::Rows(&[&[
                "'foot':B & 'ball':B & 'klubber':B & ( 'booking':A | 'book':A ) & 'sky'",
            ]]),
        },
        "95" => Outcome::Rows(&[&["( 'foot':B & 'ball':B & 'klubber':B ) <-> 'sky'"]]),
        "96" => Outcome::Rows(&[&["( 'foot' & 'ball' & 'klubber' ) <-> 'sky'"]]),
        "97" => Outcome::Success,
        "98" => Outcome::Success,
        "99" => Outcome::Success,
        "100" => Outcome::Success,
        "101" => Outcome::Rows(&[&["'call':4 'often':3 'pgsql':1,6,8,12 'pronounc':10"]]),
        "102" => Outcome::Rows(&[&["'common':2 'googl':7,10 'instead':8 'mistak':3 'write':6"]]),
        "103" => Outcome::Rows(&[&["'form':8 'index':1,3,10 'plural':7 'right':6"]]),
        "104" => Outcome::Rows(&[&["'index' & 'index':*"]]),
        "105" => Outcome::Success,
        "106" => Outcome::Success,
        "107" => Outcome::Rows(&[&["'1':1,5 '12':3 '123':4 'pgsql':2"]]),
        "108" => {
            Outcome::Rows(&[&["'abbrevi':10 'call':8 'new':4 'sn':1,9,11 'star':5 'usual':7"]])
        }
        "109" => Outcome::Rows(&[&["'card':3,10 'invit':2,9 'like':6 'look':5 'order':1,8"]]),
        "110" => Outcome::Error(ScriptError {
            message: "unrecognized Ispell parameter: \"DictFile\"",
            detail: None,
            hint: None,
            context: None,
            position: None,
        }),
        "111" => Outcome::Success,
        "112" => Outcome::Success,
        "113" => Outcome::Error(ScriptError {
            message: "token type \"not_a_token\" does not exist",
            detail: None,
            hint: None,
            context: None,
            position: None,
        }),
        "114" => Outcome::Error(ScriptError {
            message: "token type \"not_a_token\" does not exist",
            detail: None,
            hint: None,
            context: None,
            position: None,
        }),
        "115" => Outcome::Success,
        "116" => Outcome::Error(ScriptError {
            message: "mapping for token type \"word\" does not exist",
            detail: None,
            hint: None,
            context: None,
            position: None,
        }),
        "117" => Outcome::Success,
        "118" => Outcome::Success,
        "119" => Outcome::Error(ScriptError {
            message: "token type \"not_a_token\" does not exist",
            detail: None,
            hint: None,
            context: None,
            position: None,
        }),
        "120" => Outcome::Success,
        _ => return None,
    })
}
