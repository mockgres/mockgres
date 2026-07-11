use crate::db::{CellInput, Db};
use crate::engine::{DataType, EvalContext, PrimaryKeySpec, Value};
use crate::session::SessionTimeZone;
use crate::txn::SYSTEM_TXID;

mod builtins;

use builtins::BUILTIN_TYPES;
// Minimal representation of pg_catalog.pg_type entries for builtin scalar types.
#[derive(Clone, Copy)]
struct PgTypeRow {
    oid: i32,
    typname: &'static str,
    typnamespace: i32,
    typtype: &'static str,
    typcategory: &'static str,
    typdelim: &'static str,
    typinput: &'static str,
    typoutput: &'static str,
    typreceive: &'static str,
    typsend: &'static str,
    typalign: &'static str,
    typstorage: &'static str,
    typlen: i32,
    typbyval: bool,
    typdefault: Option<&'static str>,
    typnotnull: bool,
    typbasetype: i32,
    typtypmod: i32,
    typrelid: i32,
    typelem: i32,
    typarray: i32,
    typcollation: i32,
}

impl PgTypeRow {
    fn to_cells(self) -> Vec<CellInput> {
        vec![
            CellInput::Value(Value::Int64(self.oid as i64)),
            CellInput::Value(Value::Text(self.typname.to_string())),
            CellInput::Value(Value::Int64(self.typnamespace as i64)),
            CellInput::Value(Value::Bytes(self.typtype.as_bytes().to_vec())),
            CellInput::Value(Value::Text(self.typcategory.to_string())),
            CellInput::Value(Value::Text(self.typdelim.to_string())),
            CellInput::Value(Value::Text(self.typinput.to_string())),
            CellInput::Value(Value::Text(self.typoutput.to_string())),
            CellInput::Value(Value::Text(self.typreceive.to_string())),
            CellInput::Value(Value::Text(self.typsend.to_string())),
            CellInput::Value(Value::Text(self.typalign.to_string())),
            CellInput::Value(Value::Text(self.typstorage.to_string())),
            CellInput::Value(Value::Int64(self.typlen as i64)),
            CellInput::Value(Value::Bool(self.typbyval)),
            match self.typdefault {
                Some(s) => CellInput::Value(Value::Text(s.to_string())),
                None => CellInput::Value(Value::Null),
            },
            CellInput::Value(Value::Bool(self.typnotnull)),
            CellInput::Value(Value::Int64(self.typbasetype as i64)),
            CellInput::Value(Value::Int64(self.typtypmod as i64)),
            CellInput::Value(Value::Int64(self.typrelid as i64)),
            CellInput::Value(Value::Int64(self.typelem as i64)),
            CellInput::Value(Value::Int64(self.typarray as i64)),
            CellInput::Value(Value::Int64(self.typcollation as i64)),
        ]
    }
}

// OIDs aligned with upstream Postgres for the builtin types we support.
pub(super) fn init_pg_type(db: &mut Db) {
    let cols = vec![
        ("oid".to_string(), DataType::Int4, false, None, None),
        ("typname".to_string(), DataType::Name, false, None, None),
        (
            "typnamespace".to_string(),
            DataType::Int4,
            false,
            None,
            None,
        ),
        ("typtype".to_string(), DataType::Bytea, false, None, None),
        ("typcategory".to_string(), DataType::Text, false, None, None),
        ("typdelim".to_string(), DataType::Text, false, None, None),
        ("typinput".to_string(), DataType::Text, false, None, None),
        ("typoutput".to_string(), DataType::Text, false, None, None),
        ("typreceive".to_string(), DataType::Text, false, None, None),
        ("typsend".to_string(), DataType::Text, false, None, None),
        ("typalign".to_string(), DataType::Text, false, None, None),
        ("typstorage".to_string(), DataType::Text, false, None, None),
        ("typlen".to_string(), DataType::Int4, false, None, None),
        ("typbyval".to_string(), DataType::Bool, false, None, None),
        ("typdefault".to_string(), DataType::Text, true, None, None),
        ("typnotnull".to_string(), DataType::Bool, false, None, None),
        ("typbasetype".to_string(), DataType::Int4, false, None, None),
        ("typtypmod".to_string(), DataType::Int4, false, None, None),
        ("typrelid".to_string(), DataType::Int4, false, None, None),
        ("typelem".to_string(), DataType::Int4, false, None, None),
        ("typarray".to_string(), DataType::Int4, false, None, None),
        (
            "typcollation".to_string(),
            DataType::Int4,
            false,
            None,
            None,
        ),
    ];
    let pk = PrimaryKeySpec {
        name: Some("pg_type_oid_pkey".to_string()),
        columns: vec!["oid".to_string()],
    };
    db.create_table("pg_catalog", "pg_type", cols, Some(pk), Vec::new(), &[])
        .expect("create pg_catalog.pg_type");

    let ctx = EvalContext::new(SessionTimeZone::Utc);
    let rows: Vec<Vec<CellInput>> = BUILTIN_TYPES.iter().map(|row| row.to_cells()).collect();
    db.insert_full_rows(
        "pg_catalog",
        "pg_type",
        rows,
        false,
        SYSTEM_TXID,
        &[],
        &ctx,
        None,
    )
    .unwrap_or_else(|e| panic!("seed pg_catalog.pg_type: {e}"));
}
