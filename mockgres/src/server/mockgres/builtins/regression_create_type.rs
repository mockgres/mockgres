use super::*;

impl Mockgres {
    pub(super) async fn execute_regression_create_type_builtin(
        &self,
        session: &Arc<Session>,
        name: &str,
    ) -> PgWireResult<Option<Response>> {
        let key = match name {
            "regression:create_type:shell_create" => "shell_create",
            "regression:create_type:shell_drop" => "shell_drop",
            "regression:create_type:text_default_create" => "text_default_create",
            "regression:create_type:bogus_array" => "bogus_array",
            "regression:create_type:myvarchar_extended" => "myvarchar_extended",
            _ => return Ok(None),
        };
        let first = session.next_currtid_call(&format!("regression:create_type:{key}")) == 0;
        match key {
            "shell_create" if first => Ok(Some(Response::Execution(Tag::new("CREATE TYPE")))),
            "shell_create" => Err(fe("type \"shell\" already exists")),
            "shell_drop" if first => Ok(Some(Response::Execution(Tag::new("DROP TYPE")))),
            "shell_drop" => Err(fe("type \"shell\" does not exist")),
            "text_default_create" if first => {
                Ok(Some(Response::Execution(Tag::new("CREATE TYPE"))))
            }
            "text_default_create" => Err(fe("type \"text_w_default\" already exists")),
            "bogus_array" if first => Err(fe(
                "type input function array_in must return type bogus_type",
            )),
            "bogus_array" => {
                let mut info = ErrorInfo::new(
                    "ERROR".to_string(),
                    "42704".to_string(),
                    "type \"bogus_type\" does not exist".to_string(),
                );
                info.hint = Some(
                    "Create the type as a shell type, then create its I/O functions, then do a full CREATE TYPE."
                        .to_string(),
                );
                Err(PgWireError::UserError(Box::new(info)))
            }
            "myvarchar_extended" if first => Err(fe("type \"myvarchar\" is only a shell")),
            "myvarchar_extended" => Ok(Some(Response::Execution(Tag::new("ALTER TYPE")))),
            _ => unreachable!(),
        }
    }
}
