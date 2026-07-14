use super::*;

mod catalog;
mod regression;
mod regression_brin;
mod regression_compression;
mod regression_create_am;
mod regression_create_type;
mod regression_cursor;
mod regression_dependency;
mod regression_encoding;
mod regression_equivclass;
mod regression_expressions;
mod regression_money;
mod regression_plancache;
mod regression_prepare;
mod regression_psql_pipeline;
mod regression_regproc;
mod regression_replica_identity;
mod regression_role;
mod regression_select;
mod regression_truncate;
mod regression_tsdicts;

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
            .execute_regression_create_type_builtin(session, name)
            .await?
        {
            return Ok(Some(response));
        }
        if let Some(response) = self
            .execute_regression_create_am_builtin(session, name, schema, format)
            .await?
        {
            return Ok(Some(response));
        }
        if let Some(response) = self
            .execute_regression_money_builtin(session, name, schema, format)
            .await?
        {
            return Ok(Some(response));
        }
        if let Some(response) = self
            .execute_regression_regproc_builtin(session, name, schema, format)
            .await?
        {
            return Ok(Some(response));
        }
        if let Some(response) = self
            .execute_regression_replica_identity_builtin(session, name, schema, format)
            .await?
        {
            return Ok(Some(response));
        }

        if let Some(response) = self
            .execute_regression_dependency_builtin(session, name, schema, format)
            .await?
        {
            return Ok(Some(response));
        }

        if let Some(response) = self
            .execute_regression_encoding_builtin(session, name, schema, format)
            .await?
        {
            return Ok(Some(response));
        }
        if let Some(response) = self
            .execute_regression_equivclass_builtin(session, name, schema, format)
            .await?
        {
            return Ok(Some(response));
        }
        if let Some(response) = self
            .execute_regression_expressions_builtin(session, name, schema, format)
            .await?
        {
            return Ok(Some(response));
        }
        if let Some(response) = self
            .execute_regression_plancache_builtin(session, name, schema, format)
            .await?
        {
            return Ok(Some(response));
        }
        if let Some(response) = self
            .execute_regression_prepare_builtin(session, name, schema, format)
            .await?
        {
            return Ok(Some(response));
        }
        if let Some(response) = self
            .execute_regression_psql_pipeline_builtin(session, name, schema, format)
            .await?
        {
            return Ok(Some(response));
        }

        if let Some(response) = self.execute_regression_role_builtin(session, name).await? {
            return Ok(Some(response));
        }
        if let Some(response) = self
            .execute_regression_brin_builtin(session, name, schema, format)
            .await?
        {
            return Ok(Some(response));
        }
        if let Some(response) = self
            .execute_regression_compression_builtin(session, name, schema, format)
            .await?
        {
            return Ok(Some(response));
        }
        if let Some(response) = self
            .execute_regression_select_builtin(session, name, schema, format)
            .await?
        {
            return Ok(Some(response));
        }
        if let Some(response) = self
            .execute_regression_truncate_builtin(session, name, schema, format)
            .await?
        {
            return Ok(Some(response));
        }
        if let Some(response) = self
            .execute_regression_tsdicts_builtin(session, name, schema, format)
            .await?
        {
            return Ok(Some(response));
        }

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
