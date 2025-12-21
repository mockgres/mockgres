use crate::catalog::SchemaName;

use super::*;

impl Db {
    pub fn create_schema(&mut self, name: &str, if_not_exists: bool) -> anyhow::Result<()> {
        if self.catalog.schema_id(name).is_some() {
            return if if_not_exists {
                Ok(())
            } else {
                Err(sql_err(
                    "42P06",
                    format!("schema \"{}\" already exists", name),
                ))
            };
        }
        let id = self.catalog.ensure_schema(name);
        self.insert_pg_namespace_row(id, name);
        Ok(())
    }

    pub fn drop_schema(
        &mut self,
        name: &str,
        cascade: bool,
        if_exists: bool,
    ) -> anyhow::Result<()> {
        let schema_id = match self.catalog.schema_id(name) {
            Some(id) => id,
            None => {
                return if if_exists {
                    Ok(())
                } else {
                    Err(sql_err(
                        "3F000",
                        format!("schema \"{}\" does not exist", name),
                    ))
                };
            }
        };
        let table_names: Vec<String> = self
            .catalog
            .schemas
            .get(&schema_id)
            .map(|entry| entry.objects.keys().cloned().collect())
            .unwrap_or_default();
        if !cascade && !table_names.is_empty() {
            return Err(sql_err(
                "2BP01",
                format!("cannot drop schema {} because it is not empty", name),
            ));
        }
        for table_name in table_names {
            self.drop_table(name, &table_name, false, cascade)?;
        }
        self.catalog.drop_schema_entry(schema_id);
        self.remove_pg_namespace_row(schema_id);
        Ok(())
    }

    pub fn rename_schema(&mut self, old: &str, new: &str) -> anyhow::Result<()> {
        let schema_id = self
            .catalog
            .schema_id(old)
            .ok_or_else(|| sql_err("3F000", format!("schema \"{}\" does not exist", old)))?;
        if self.catalog.schema_id(new).is_some() {
            return Err(sql_err(
                "42P06",
                format!("schema \"{}\" already exists", new),
            ));
        }
        self.catalog
            .rename_schema_entry(schema_id, SchemaName::new(new));
        Ok(())
    }

    pub fn drop_table(
        &mut self,
        schema: &str,
        name: &str,
        if_exists: bool,
        cascade: bool,
    ) -> anyhow::Result<()> {
        let table_exists = self.catalog.get_table(schema, name).is_some();
        if !table_exists {
            return if if_exists {
                Ok(())
            } else {
                Err(sql_err("42P01", format!("no such table {schema}.{name}")))
            };
        }

        let inbound = collect_inbound_foreign_keys(&self.catalog, schema, name);
        if cascade {
            drop_inbound_foreign_keys(&mut self.catalog, &mut self.tables, &inbound)?;
        } else if let Some(fk) = inbound.first() {
            return Err(sql_err(
                "2BP01",
                format!(
                    "cannot drop table {}.{} because it is referenced by {}.{}",
                    schema, name, fk.schema, fk.table
                ),
            ));
        }

        let schema_id = match self.catalog.schema_id(schema) {
            Some(id) => id,
            None => {
                return Err(sql_err("3F000", format!("no such schema {schema}")));
            }
        };
        self.remove_pg_class_row(schema_id, name);
        match self.catalog.remove_table(schema_id, name) {
            Some(meta) => {
                if self.tables.remove(&meta.id).is_none() {
                    return Err(sql_err(
                        "XX000",
                        format!("missing storage for table id {}", meta.id),
                    ));
                }
                Ok(())
            }
            None => Ok(()),
        }
    }
}
