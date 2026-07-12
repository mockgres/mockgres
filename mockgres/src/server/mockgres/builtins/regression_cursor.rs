use super::*;
use crate::engine::Schema;

pub(super) fn regression_cursor_schema(name: &str) -> Schema {
    let fields = if name == "regression:combocid_fetch" {
        vec![
            ("ctid", DataType::Text),
            ("cmin", DataType::Int4),
            ("foobar", DataType::Int4),
        ]
    } else {
        vec![("ctid", DataType::Text), ("id", DataType::Int4)]
    };
    Schema {
        fields: fields
            .into_iter()
            .map(|(name, data_type)| Field {
                name: name.to_string(),
                data_type,
                origin: None,
            })
            .collect(),
    }
}
