use super::*;

impl Mockgres {
    pub(super) async fn execute_regression_role_builtin(
        &self,
        session: &Arc<Session>,
        name: &str,
    ) -> PgWireResult<Option<Response>> {
        let Some(spec) = name.strip_prefix("regression:role_sequence:") else {
            return Ok(None);
        };
        let mut parts = spec.splitn(4, ':');
        let key = parts
            .next()
            .ok_or_else(|| fe("missing role sequence key"))?;
        let tag = parts
            .next()
            .ok_or_else(|| fe("missing role sequence tag"))?;
        let error_at = parts
            .next()
            .ok_or_else(|| fe("missing role sequence error index"))?
            .parse::<u32>()
            .map_err(|_| fe("invalid role sequence error index"))?;
        let error = parts
            .next()
            .ok_or_else(|| fe("missing role sequence error"))?;
        let call = session.next_currtid_call(&format!("regression:role_sequence:{key}"));
        if call == error_at {
            if let Some((message, detail)) = error.split_once('|') {
                let mut info = ErrorInfo::new(
                    "ERROR".to_string(),
                    "42501".to_string(),
                    message.to_string(),
                );
                info.detail = Some(detail.to_string());
                return Err(PgWireError::UserError(Box::new(info)));
            }
            return Err(fe(error));
        }
        Ok(Some(Response::Execution(Tag::new(&tag.replace('_', " ")))))
    }
}
