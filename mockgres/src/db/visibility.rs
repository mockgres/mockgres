use crate::storage::{Row, RowId, RowKey, Table, VersionedRow};
use crate::txn::VisibilityContext;

use super::sql_err;

pub(crate) fn row_key_to_row_id(key: &RowKey) -> anyhow::Result<RowId> {
    if let RowKey::RowId(id) = key {
        Ok(*id)
    } else {
        Err(sql_err(
            "XX000",
            "expected physical RowId key but found primary key tuple",
        ))
    }
}

pub(crate) fn select_visible_version_idx(
    versions: &[VersionedRow],
    visibility: &VisibilityContext,
) -> Option<usize> {
    versions
        .iter()
        .enumerate()
        .rev()
        .find(|(_, version)| visibility.is_visible(version))
        .map(|(idx, _)| idx)
}

pub(crate) fn select_visible_version<'a>(
    versions: &'a [VersionedRow],
    visibility: &VisibilityContext,
) -> Option<&'a VersionedRow> {
    select_visible_version_idx(versions, visibility).map(|idx| &versions[idx])
}

pub(crate) fn visible_row_clone(
    table: &Table,
    row_id: RowId,
    visibility: &VisibilityContext,
) -> Option<Row> {
    table
        .rows_by_key
        .get(&RowKey::RowId(row_id))
        .and_then(|versions| select_visible_version(versions, visibility))
        .map(|version| version.data.clone())
}
