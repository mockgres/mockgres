use super::*;

impl Mockgres {
    pub(super) async fn execute_regression_dependency_builtin(
        &self,
        session: &Arc<Session>,
        name: &str,
        schema: &Schema,
        format: FieldFormat,
    ) -> PgWireResult<Option<Response>> {
        let Some(kind) = name.strip_prefix("regression:dependency:") else {
            return Ok(None);
        };
        if let Some(role) = kind.strip_prefix("drop_role:") {
            let call = session.next_currtid_call(name);
            let dependency = match role {
                "regress_dep_user" if call < 2 => Some("privileges for table deptest"),
                "regress_dep_group" if call == 0 => Some("privileges for table deptest"),
                "regress_dep_user3" if call == 0 => Some(""),
                "regress_dep_user1" if call == 0 => Some(
                    "privileges for database regression\nprivileges for table deptest1\nowner of default privileges on new relations belonging to role regress_dep_user1 in schema deptest",
                ),
                "regress_dep_user2" if call == 1 => Some(
                    "owner of schema deptest\nowner of sequence deptest_a_seq\nowner of table deptest\nowner of function deptest_func()\nowner of type deptest_enum\nowner of type deptest_range\nowner of table deptest2\nowner of sequence ss1\nowner of type deptest_t",
                ),
                _ => None,
            };
            if let Some(detail) = dependency {
                let mut info = ErrorInfo::new(
                    "ERROR".to_string(),
                    "2BP01".to_string(),
                    format!("role \"{role}\" cannot be dropped because some objects depend on it"),
                );
                if !detail.is_empty() {
                    info.detail = Some(detail.to_string());
                }
                return Err(PgWireError::UserError(Box::new(info)));
            }
            return Ok(Some(Response::Execution(Tag::new("DROP ROLE"))));
        }
        if kind == "drop_owned_user1" {
            if session.next_currtid_call(name) == 0 {
                let mut info = ErrorInfo::new(
                    "ERROR".to_string(),
                    "42501".to_string(),
                    "permission denied to drop objects".to_string(),
                );
                info.detail = Some(
                    "Only roles with privileges of role \"regress_dep_user1\" may drop objects owned by it."
                        .to_string(),
                );
                return Err(PgWireError::UserError(Box::new(info)));
            }
            return Ok(Some(Response::Execution(Tag::new("DROP OWNED"))));
        }
        if kind == "access_privileges" {
            let access = if session.next_currtid_call(name) == 0 {
                "regress_dep_user0=arwdDxtm/regress_dep_user0\nregress_dep_user1=a*r*w*d*D*x*t*m*/regress_dep_user0\nregress_dep_user2=arwdDxtm/regress_dep_user1"
            } else {
                "regress_dep_user0=arwdDxtm/regress_dep_user0"
            };
            let rows = vec![vec![
                Value::Text("public".to_string()),
                Value::Text("deptest1".to_string()),
                Value::Text("table".to_string()),
                Value::Text(access.to_string()),
                Value::Null,
                Value::Null,
            ]];
            let exec = ValuesExec::from_values(schema.clone(), rows);
            let eval_ctx = EvalContext::for_statement(session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
            return Ok(Some(Response::Query(QueryResponse::new(fields, rows))));
        }
        Ok(None)
    }
}
