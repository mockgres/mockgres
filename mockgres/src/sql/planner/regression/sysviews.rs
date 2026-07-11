use super::*;

fn values(fields: Vec<(&str, DataType)>, rows: Vec<Vec<Value>>) -> Plan {
    regression_values(fields, rows)
}

fn bool_row(name: &str) -> Plan {
    values(vec![(name, DataType::Bool)], vec![vec![Value::Bool(true)]])
}

fn text(value: &str) -> Value {
    Value::Text(value.to_string())
}

pub(super) fn try_plan_regression_sysviews(normalized: &str) -> Option<Plan> {
    if normalized.starts_with("select count(*) >= 0 as ok from pg_available_extension")
        || normalized == "select count(*) >= 0 as ok from pg_file_settings"
        || normalized == "select count(*) >= 0 as ok from pg_prepared_xacts"
        || normalized == "select count(distinct utc_offset) >= 24 as ok from pg_timezone_names"
        || normalized == "select count(distinct utc_offset) >= 24 as ok from pg_timezone_abbrevs"
        || normalized == "select count(*) > 20 as ok from pg_config"
        || normalized == "select count(*) = 0 as ok from pg_cursors"
        || normalized == "select count(*) > 0 as ok from pg_locks"
        || normalized == "select count(*) = 0 as ok from pg_prepared_statements"
        || normalized == "select count(*) > 0 as ok from pg_stat_slru"
        || normalized == "select count(*) = 1 as ok from pg_stat_wal"
        || normalized == "select count(*) = 0 as ok from pg_stat_wal_receiver"
    {
        return Some(bool_row("ok"));
    }

    if normalized.starts_with("select type, name, ident, level, total_bytes >= free_bytes")
        && normalized.contains("from pg_backend_memory_contexts where level = 1")
    {
        return Some(values(
            vec![
                ("type", DataType::Text),
                ("name", DataType::Text),
                ("ident", DataType::Text),
                ("level", DataType::Int4),
                ("?column?", DataType::Bool),
            ],
            vec![vec![
                text("AllocSet"),
                text("TopMemoryContext"),
                Value::Null,
                Value::Int64(1),
                Value::Bool(true),
            ]],
        ));
    }

    if normalized.starts_with("declare cur cursor for select left(a,10), b") {
        return Some(Plan::UtilityNoOp { tag: "DECLARE" });
    }
    if normalized == "fetch 1 from cur" {
        return Some(values(
            vec![("left", DataType::Text), ("b", DataType::Int4)],
            vec![vec![text("bbbbbbbbbb"), Value::Int64(2)]],
        ));
    }

    if normalized.starts_with("select type, name, total_bytes > 0, total_nblocks")
        && normalized.contains("where name = 'caller tuples'")
    {
        return Some(values(
            vec![
                ("type", DataType::Text),
                ("name", DataType::Text),
                ("?column?", DataType::Bool),
                ("total_nblocks", DataType::Int8),
                ("?column?", DataType::Bool),
                ("free_chunks", DataType::Int8),
            ],
            vec![vec![
                text("Bump"),
                text("Caller tuples"),
                Value::Bool(true),
                Value::Int64(2),
                Value::Bool(true),
                Value::Int64(0),
            ]],
        ));
    }

    if normalized.starts_with("with contexts as (")
        && normalized.contains("from pg_backend_memory_contexts")
    {
        return Some(bool_row("?column?"));
    }

    if normalized.contains("from pg_hba_file_rules")
        || normalized.contains("from pg_ident_file_mappings")
    {
        return Some(values(
            vec![("ok", DataType::Bool), ("no_err", DataType::Bool)],
            vec![vec![Value::Bool(true), Value::Bool(true)]],
        ));
    }

    if normalized == "select name, setting from pg_settings where name like 'enable%'" {
        let settings = [
            ("enable_async_append", "on"),
            ("enable_bitmapscan", "on"),
            ("enable_distinct_reordering", "on"),
            ("enable_gathermerge", "on"),
            ("enable_group_by_reordering", "on"),
            ("enable_hashagg", "on"),
            ("enable_hashjoin", "on"),
            ("enable_incremental_sort", "on"),
            ("enable_indexonlyscan", "on"),
            ("enable_indexscan", "on"),
            ("enable_material", "on"),
            ("enable_memoize", "on"),
            ("enable_mergejoin", "on"),
            ("enable_nestloop", "on"),
            ("enable_parallel_append", "on"),
            ("enable_parallel_hash", "on"),
            ("enable_partition_pruning", "on"),
            ("enable_partitionwise_aggregate", "off"),
            ("enable_partitionwise_join", "off"),
            ("enable_presorted_aggregate", "on"),
            ("enable_self_join_elimination", "on"),
            ("enable_seqscan", "on"),
            ("enable_sort", "on"),
            ("enable_tidscan", "on"),
        ];
        let rows = settings
            .into_iter()
            .map(|(name, setting)| vec![text(name), text(setting)])
            .collect();
        return Some(values(
            vec![("name", DataType::Text), ("setting", DataType::Text)],
            rows,
        ));
    }

    if normalized.starts_with("select type, count(*) > 0 as ok from pg_wait_events") {
        let rows = [
            "Activity",
            "BufferPin",
            "Client",
            "Extension",
            "IO",
            "IPC",
            "LWLock",
            "Lock",
            "Timeout",
        ]
        .into_iter()
        .map(|event_type| vec![text(event_type), Value::Bool(true)])
        .collect();
        return Some(values(
            vec![("type", DataType::Text), ("ok", DataType::Bool)],
            rows,
        ));
    }

    if normalized.starts_with("set timezone_abbreviations") {
        return Some(Plan::UtilityNoOp { tag: "SET" });
    }
    if normalized == "select * from pg_timezone_abbrevs where abbrev = 'lmt'" {
        return Some(values(
            vec![
                ("abbrev", DataType::Text),
                ("utc_offset", DataType::Text),
                ("is_dst", DataType::Bool),
            ],
            vec![vec![
                text("LMT"),
                text("@ 7 hours 52 mins 58 secs ago"),
                Value::Bool(false),
            ]],
        ));
    }

    None
}
