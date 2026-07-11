use std::path::Path;
use std::sync::Arc;

use parking_lot::RwLock;
use pgwire::error::PgWireResult;

use crate::db::{CellInput, Db};
use crate::engine::{EvalContext, ExecNode, ObjName, Schema, Value, ValuesExec, fe_code};
use crate::server::errors::map_db_err;
use crate::server::exec_builder::schema_or_public;
use crate::session::Session;
use crate::txn::TransactionManager;

use super::tx::{finish_writer_tx, writer_txid};

type ExecResult = PgWireResult<(Box<dyn ExecNode>, Option<String>, Option<usize>)>;

pub(crate) fn build_copy_from_executor(
    db: &Arc<RwLock<Db>>,
    txn_manager: &Arc<TransactionManager>,
    session: &Arc<Session>,
    table: &ObjName,
    columns: &Option<Vec<String>>,
    filename: &str,
    ctx: &EvalContext,
) -> ExecResult {
    let schema_name = schema_or_public(&table.schema);
    let table_meta = {
        let db = db.read();
        db.resolve_table(schema_name, &table.name)
            .map_err(map_db_err)?
            .clone()
    };
    let column_indexes = match columns {
        Some(columns) => columns
            .iter()
            .map(|column| {
                table_meta
                    .columns
                    .iter()
                    .position(|candidate| candidate.name == *column)
                    .ok_or_else(|| fe_code("42703", format!("unknown column: {column}")))
            })
            .collect::<PgWireResult<Vec<_>>>()?,
        None => (0..table_meta.columns.len()).collect(),
    };
    let records = read_copy_text_file(filename)?;
    let mut rows = Vec::with_capacity(records.len());
    for (line_index, record) in records.into_iter().enumerate() {
        let line_number = line_index + 1;
        if record.len() < column_indexes.len() {
            let missing_column = &table_meta.columns[column_indexes[record.len()]].name;
            return Err(fe_code(
                "22P04",
                format!("missing data for column \"{missing_column}\" on COPY line {line_number}"),
            ));
        }
        if record.len() > column_indexes.len() {
            return Err(fe_code(
                "22P04",
                format!("extra data after last expected column on COPY line {line_number}"),
            ));
        }

        let mut row = vec![CellInput::Default; table_meta.columns.len()];
        for (value, column_index) in record.into_iter().zip(&column_indexes) {
            row[*column_index] = CellInput::Value(match value {
                Some(value) => Value::Text(value),
                None => Value::Null,
            });
        }
        rows.push(row);
    }

    let (txid, autocommit) = writer_txid(session, txn_manager);
    let result = {
        let mut db = db.write();
        db.insert_full_rows(schema_name, &table.name, rows, false, txid, &[], ctx, None)
    };
    let (inserted, _, inserted_ptrs, _, _) = match result {
        Ok(result) => result,
        Err(error) => {
            finish_writer_tx(txn_manager, txid, autocommit, false);
            return Err(map_db_err(error));
        }
    };
    if autocommit {
        finish_writer_tx(txn_manager, txid, true, true);
    } else {
        session.record_inserts(inserted_ptrs);
    }

    Ok((
        Box::new(ValuesExec::new(Schema { fields: vec![] }, vec![])?),
        Some(format!("COPY {inserted}")),
        Some(inserted),
    ))
}

fn read_copy_text_file(filename: &str) -> PgWireResult<Vec<Vec<Option<String>>>> {
    if !Path::new(filename).is_absolute() {
        return Err(fe_code(
            "42602",
            "relative path not allowed for COPY to file",
        ));
    }
    let bytes = std::fs::read(filename).map_err(|error| {
        fe_code(
            "58P01",
            format!("could not open file \"{filename}\" for reading: {error}"),
        )
    })?;
    let contents = std::str::from_utf8(&bytes).map_err(|error| {
        fe_code(
            "22021",
            format!("invalid byte sequence for encoding UTF8 in COPY file: {error}"),
        )
    })?;
    contents
        .lines()
        .enumerate()
        .map(|(line, record)| parse_copy_text_record(record, line + 1))
        .collect()
}

fn parse_copy_text_record(record: &str, line_number: usize) -> PgWireResult<Vec<Option<String>>> {
    let bytes = record.as_bytes();
    let mut fields = Vec::new();
    let mut raw = Vec::new();
    let mut decoded = Vec::new();
    let mut index = 0;

    while index < bytes.len() {
        match bytes[index] {
            b'\t' => {
                fields.push(finish_copy_field(&raw, &decoded, line_number)?);
                raw.clear();
                decoded.clear();
                index += 1;
            }
            b'\\' => {
                raw.push(b'\\');
                index += 1;
                if index >= bytes.len() {
                    return Err(fe_code(
                        "22P04",
                        format!("unterminated COPY escape on line {line_number}"),
                    ));
                }
                let escape = bytes[index];
                raw.push(escape);
                index += 1;
                match escape {
                    b'b' => decoded.push(0x08),
                    b'f' => decoded.push(0x0c),
                    b'n' => decoded.push(b'\n'),
                    b'r' => decoded.push(b'\r'),
                    b't' => decoded.push(b'\t'),
                    b'v' => decoded.push(0x0b),
                    b'0'..=b'7' => {
                        let mut value = u16::from(escape - b'0');
                        for _ in 0..2 {
                            if index >= bytes.len() || !matches!(bytes[index], b'0'..=b'7') {
                                break;
                            }
                            raw.push(bytes[index]);
                            value = value * 8 + u16::from(bytes[index] - b'0');
                            index += 1;
                        }
                        decoded.push(value as u8);
                    }
                    b'x' => {
                        let mut value = 0_u8;
                        let mut digits = 0;
                        while digits < 2 && index < bytes.len() {
                            let Some(digit) = hex_value(bytes[index]) else {
                                break;
                            };
                            raw.push(bytes[index]);
                            value = value * 16 + digit;
                            index += 1;
                            digits += 1;
                        }
                        if digits == 0 {
                            decoded.push(b'x');
                        } else {
                            decoded.push(value);
                        }
                    }
                    other => decoded.push(other),
                }
            }
            byte => {
                raw.push(byte);
                decoded.push(byte);
                index += 1;
            }
        }
    }
    fields.push(finish_copy_field(&raw, &decoded, line_number)?);
    Ok(fields)
}

fn finish_copy_field(
    raw: &[u8],
    decoded: &[u8],
    line_number: usize,
) -> PgWireResult<Option<String>> {
    if raw == b"\\N" {
        return Ok(None);
    }
    if decoded.contains(&0) {
        return Err(fe_code(
            "22021",
            format!("invalid byte sequence for encoding UTF8 on COPY line {line_number}: 0x00"),
        ));
    }
    let value = std::str::from_utf8(decoded).map_err(|error| {
        fe_code(
            "22021",
            format!("invalid byte sequence for encoding UTF8 on COPY line {line_number}: {error}"),
        )
    })?;
    Ok(Some(value.to_string()))
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}
