use super::*;

impl Db {
    pub fn rollback_transaction_changes(
        &mut self,
        changes: &TxnChanges,
        txid: TxId,
    ) -> anyhow::Result<()> {
        self.rollback_inserts(&changes.inserted, txid)?;
        self.rollback_xmax(&changes.updated_old, txid)?;
        Ok(())
    }

    fn rollback_inserts(&mut self, ptrs: &[RowPointer], txid: TxId) -> anyhow::Result<()> {
        for ptr in ptrs {
            let Some(meta) = self.table_meta_by_id(ptr.table_id).cloned() else {
                continue;
            };
            if let Some(mut table) = self.tables.remove(&ptr.table_id) {
                let mut removed_rows = Vec::new();
                if let Some(versions) = table.rows_by_key.get_mut(&ptr.key) {
                    versions.retain(|v| {
                        if v.xmin == txid {
                            removed_rows.push(v.data.clone());
                            false
                        } else {
                            true
                        }
                    });
                    if versions.is_empty() {
                        table.rows_by_key.remove(&ptr.key);
                    }
                }
                if !removed_rows.is_empty() {
                    let row_id = row_key_to_row_id(&ptr.key)?;
                    for row in removed_rows {
                        if let Some(pk_key) = build_primary_key_row_key(&meta, &row)?
                            && let Some(pk_map) = table.pk_map.as_mut()
                            && let Some(existing) = pk_map.get(&pk_key)
                            && *existing == row_id
                        {
                            pk_map.remove(&pk_key);
                        }
                        let fk_keys = meta
                            .foreign_keys
                            .iter()
                            .map(|fk| build_fk_parent_key(&row, fk))
                            .collect::<Vec<_>>();
                        remove_fk_rev_entries(&mut table, &meta, row_id, &fk_keys);
                        let unique_keys = build_unique_index_values(&meta, &row);
                        remove_unique_entries(&mut table, &unique_keys, row_id);
                    }
                }
                self.tables.insert(ptr.table_id, table);
            } else {
                return Err(sql_err(
                    "XX000",
                    format!("missing storage for table id {}", ptr.table_id),
                ));
            }
        }
        Ok(())
    }

    fn rollback_xmax(&mut self, ptrs: &[RowPointer], txid: TxId) -> anyhow::Result<()> {
        for ptr in ptrs {
            let Some(meta) = self.table_meta_by_id(ptr.table_id).cloned() else {
                continue;
            };
            if let Some(mut table) = self.tables.remove(&ptr.table_id) {
                let mut restored_rows = Vec::new();
                if let Some(versions) = table.rows_by_key.get_mut(&ptr.key) {
                    for version in versions.iter_mut() {
                        if version.xmax == Some(txid) {
                            version.xmax = None;
                            restored_rows.push(version.data.clone());
                        }
                    }
                }
                if !restored_rows.is_empty() {
                    let row_id = row_key_to_row_id(&ptr.key)?;
                    for row in restored_rows {
                        if let Some(pk_key) = build_primary_key_row_key(&meta, &row)?
                            && let Some(pk_map) = table.pk_map.as_mut()
                        {
                            pk_map.insert(pk_key, row_id);
                        }
                        let fk_keys = meta
                            .foreign_keys
                            .iter()
                            .map(|fk| build_fk_parent_key(&row, fk))
                            .collect::<Vec<_>>();
                        add_fk_rev_entries(&mut table, &meta, row_id, &fk_keys);
                        let unique_keys = build_unique_index_values(&meta, &row);
                        insert_unique_entries_owned(&mut table, unique_keys, row_id);
                    }
                }
                self.tables.insert(ptr.table_id, table);
            } else {
                return Err(sql_err(
                    "XX000",
                    format!("missing storage for table id {}", ptr.table_id),
                ));
            }
        }
        Ok(())
    }
}
