use pgwire::error::PgWireError;

use crate::engine::{SqlError, fe, fe_code};

pub fn map_db_err(err: anyhow::Error) -> PgWireError {
    if let Some(sql) = err.downcast_ref::<SqlError>() {
        fe_code(sql.code, sql.message.clone())
    } else {
        fe(err.to_string())
    }
}
