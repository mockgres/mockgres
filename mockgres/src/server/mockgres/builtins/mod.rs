use super::*;

mod catalog;
mod regression;

impl Mockgres {
    pub(super) async fn execute_builtin_statement(
        &self,
        session: &Arc<Session>,
        plan: &Plan,
        format: FieldFormat,
    ) -> PgWireResult<Option<Response>> {
        if let Plan::CreateDatabase { name } = plan {
            if session.current_tx().is_some() {
                return Err(fe_code(
                    "25001",
                    "CREATE DATABASE cannot run inside a transaction block",
                ));
            }

            let mut databases = self.databases.write();
            if databases.contains_key(name) {
                return Err(fe_code(
                    "42P04",
                    format!("database \"{name}\" already exists"),
                ));
            }
            databases.insert(name.clone(), Arc::new(RwLock::new(Db::default())));
            return Ok(Some(Response::Execution(Tag::new("CREATE DATABASE"))));
        }

        let Plan::CallBuiltin { name, schema, .. } = plan else {
            return Ok(None);
        };

        if let Some(response) = self
            .execute_regression_builtin(session, name, schema, format)
            .await?
        {
            return Ok(Some(response));
        }
        if let Some(response) = self
            .execute_catalog_builtin(session, name, schema, format)
            .await?
        {
            return Ok(Some(response));
        }
        Ok(None)
    }
}
