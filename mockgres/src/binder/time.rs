use crate::engine::{ScalarExpr, ScalarFunc, Value, fe};
use crate::session::now_utc_micros;
use crate::types::timestamp_micros_to_date_days;
use pgwire::error::PgWireResult;

#[derive(Clone, Copy)]
pub(crate) struct BindTimeContext {
    statement_time_micros: Option<i64>,
    txn_start_micros: Option<i64>,
}

impl BindTimeContext {
    pub(crate) fn new(statement_time_micros: Option<i64>, txn_start_micros: Option<i64>) -> Self {
        Self {
            statement_time_micros,
            txn_start_micros,
        }
    }

    pub(crate) fn statement_or_now(&self) -> i64 {
        self.statement_time_micros.unwrap_or_else(now_utc_micros)
    }

    pub(crate) fn txn_or_statement_or_now(&self) -> i64 {
        self.txn_start_micros
            .or(self.statement_time_micros)
            .unwrap_or_else(now_utc_micros)
    }
}

pub(crate) fn bind_time_scalar_func(
    func: ScalarFunc,
    args: &[ScalarExpr],
    time_ctx: BindTimeContext,
) -> Option<PgWireResult<ScalarExpr>> {
    match func {
        ScalarFunc::Now | ScalarFunc::CurrentTimestamp => {
            Some(bind_current_timestamp(args, time_ctx))
        }
        ScalarFunc::StatementTimestamp => Some(bind_statement_timestamp(args, time_ctx)),
        ScalarFunc::TransactionTimestamp => Some(bind_transaction_timestamp(args, time_ctx)),
        ScalarFunc::ClockTimestamp => Some(bind_clock_timestamp(args)),
        ScalarFunc::CurrentDate => Some(bind_current_date(args, time_ctx)),
        _ => None,
    }
}

fn bind_current_timestamp(
    args: &[ScalarExpr],
    time_ctx: BindTimeContext,
) -> PgWireResult<ScalarExpr> {
    if !args.is_empty() {
        return Err(fe("current_timestamp does not take arguments"));
    }
    Ok(ScalarExpr::Literal(Value::TimestamptzMicros(
        time_ctx.txn_or_statement_or_now(),
    )))
}

fn bind_statement_timestamp(
    args: &[ScalarExpr],
    time_ctx: BindTimeContext,
) -> PgWireResult<ScalarExpr> {
    if !args.is_empty() {
        return Err(fe("statement_timestamp() takes no arguments"));
    }
    Ok(ScalarExpr::Literal(Value::TimestamptzMicros(
        time_ctx.statement_or_now(),
    )))
}

fn bind_transaction_timestamp(
    args: &[ScalarExpr],
    time_ctx: BindTimeContext,
) -> PgWireResult<ScalarExpr> {
    if !args.is_empty() {
        return Err(fe("transaction_timestamp() takes no arguments"));
    }
    Ok(ScalarExpr::Literal(Value::TimestamptzMicros(
        time_ctx.txn_or_statement_or_now(),
    )))
}

fn bind_clock_timestamp(args: &[ScalarExpr]) -> PgWireResult<ScalarExpr> {
    if !args.is_empty() {
        return Err(fe("clock_timestamp() takes no arguments"));
    }
    Ok(ScalarExpr::Literal(Value::TimestamptzMicros(
        now_utc_micros(),
    )))
}

fn bind_current_date(args: &[ScalarExpr], time_ctx: BindTimeContext) -> PgWireResult<ScalarExpr> {
    if !args.is_empty() {
        return Err(fe("current_date takes no arguments"));
    }
    let micros = time_ctx.statement_or_now();
    let days = timestamp_micros_to_date_days(micros).map_err(fe)?;
    Ok(ScalarExpr::Literal(Value::Date(days)))
}
