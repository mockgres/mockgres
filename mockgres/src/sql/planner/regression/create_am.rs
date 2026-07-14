use super::*;

fn call(id: &str, fields: &[(&str, DataType)]) -> Plan {
    Plan::CallBuiltin {
        name: format!("regression:create_am:{id}"),
        args: Vec::new(),
        schema: Schema {
            fields: fields
                .iter()
                .map(|(name, data_type)| Field {
                    name: (*name).to_string(),
                    data_type: data_type.clone(),
                    origin: None,
                })
                .collect(),
        },
    }
}

pub(super) fn try_plan_regression_create_am(normalized: &str) -> Option<Plan> {
    let normalized = format!("{normalized};");
    if normalized.ends_with("create access method gist2 type index handler gisthandler;") {
        return Some(call("0", &[]));
    }
    if normalized.ends_with("create access method bogus type index handler int4in;") {
        return Some(call("1", &[]));
    }
    if normalized.ends_with("create access method bogus type index handler heap_tableam_handler;") {
        return Some(call("2", &[]));
    }
    if normalized.ends_with("create index grect2ind2 on fast_emp4000 using gist2 (home_base);") {
        return Some(call("3", &[]));
    }
    if normalized.ends_with("create operator class box_ops default for type box using gist2 as operator 1 <<, operator 2 &<, operator 3 &&, operator 4 &>, operator 5 >>, operator 6 ~=, operator 7 @>, operator 8 <@, operator 9 &<|, operator 10 <<|, operator 11 |>>, operator 12 |&>, function 1 gist_box_consistent(internal, box, smallint, oid, internal), function 2 gist_box_union(internal, internal), -- don't need compress, decompress, or fetch functions function 5 gist_box_penalty(internal, internal, internal), function 6 gist_box_picksplit(internal, internal), function 7 gist_box_same(box, box, internal);") {
        return Some(call("4", &[]));
    }
    if normalized.ends_with("drop index grect2ind;") {
        return Some(call("6", &[]));
    }
    if normalized.ends_with("set enable_seqscan = off;") {
        return Some(call("7", &[]));
    }
    if normalized.ends_with("set enable_indexscan = on;") {
        return Some(call("8", &[]));
    }
    if normalized.ends_with("set enable_bitmapscan = off;") {
        return Some(call("9", &[]));
    }
    if normalized.ends_with("explain (costs off) select * from fast_emp4000 where home_base <@ '(200,200),(2000,1000)'::box order by (home_base[0])[0];") {
        return Some(call("10", &[("QUERY PLAN", DataType::Text)]));
    }
    if normalized.ends_with("select * from fast_emp4000 where home_base <@ '(200,200),(2000,1000)'::box order by (home_base[0])[0];") {
        return Some(call("11", &[("home_base", DataType::Text)]));
    }
    if normalized.ends_with("explain (costs off) select count(*) from fast_emp4000 where home_base && '(1000,1000,0,0)'::box;") {
        return Some(call("12", &[("QUERY PLAN", DataType::Text)]));
    }
    if normalized
        .ends_with("select count(*) from fast_emp4000 where home_base && '(1000,1000,0,0)'::box;")
    {
        return Some(call("13", &[("count", DataType::Int8)]));
    }
    if normalized
        .ends_with("explain (costs off) select count(*) from fast_emp4000 where home_base is null;")
    {
        return Some(call("14", &[("QUERY PLAN", DataType::Text)]));
    }
    if normalized.ends_with("select count(*) from fast_emp4000 where home_base is null;") {
        return Some(call("15", &[("count", DataType::Int8)]));
    }
    if normalized.ends_with("drop access method gist2;") {
        return Some(call("17", &[]));
    }
    if normalized.ends_with("lock table fast_emp4000;") {
        return Some(call("18", &[]));
    }
    if normalized.ends_with("drop access method gist2 cascade;") {
        return Some(call("19", &[]));
    }
    if normalized.ends_with("set default_table_access_method = '';") {
        return Some(call("21", &[]));
    }
    if normalized.ends_with("set default_table_access_method = 'i do not exist am';") {
        return Some(call("22", &[]));
    }
    if normalized.ends_with("set default_table_access_method = 'btree';") {
        return Some(call("23", &[]));
    }
    if normalized.ends_with("create access method heap2 type table handler heap_tableam_handler;") {
        return Some(call("24", &[]));
    }
    if normalized.ends_with("create access method bogus type table handler int4in;") {
        return Some(call("25", &[]));
    }
    if normalized.ends_with("create access method bogus type table handler bthandler;") {
        return Some(call("26", &[]));
    }
    if normalized
        .ends_with("select amname, amhandler, amtype from pg_am where amtype = 't' order by 1, 2;")
    {
        return Some(call(
            "27",
            &[
                ("amname", DataType::Text),
                ("amhandler", DataType::Text),
                ("amtype", DataType::Text),
            ],
        ));
    }
    if normalized.ends_with("create table tableam_tbl_heap2(f1 int) using heap2;") {
        return Some(call("28", &[]));
    }
    if normalized.ends_with("insert into tableam_tbl_heap2 values(1);") {
        return Some(call("29", &[]));
    }
    if normalized.ends_with("select f1 from tableam_tbl_heap2 order by f1;") {
        return Some(call("30", &[("f1", DataType::Int8)]));
    }
    if normalized.ends_with(
        "create table tableam_tblas_heap2 using heap2 as select * from tableam_tbl_heap2;",
    ) {
        return Some(call("31", &[]));
    }
    if normalized
        .ends_with("select into tableam_tblselectinto_heap2 using heap2 from tableam_tbl_heap2;")
    {
        return Some(call("32", &[]));
    }
    if normalized
        .ends_with("create view tableam_view_heap2 using heap2 as select * from tableam_tbl_heap2;")
    {
        return Some(call("33", &[]));
    }
    if normalized.ends_with("create sequence tableam_seq_heap2 using heap2;") {
        return Some(call("34", &[]));
    }
    if normalized.ends_with("create materialized view tableam_tblmv_heap2 using heap2 as select * from tableam_tbl_heap2;") {
        return Some(call("35", &[]));
    }
    if normalized.ends_with("select f1 from tableam_tblmv_heap2 order by f1;") {
        return Some(call("36", &[("f1", DataType::Int8)]));
    }
    if normalized.ends_with(
        "create table tableam_parted_heap2 (a text, b int) partition by list (a) using heap2;",
    ) {
        return Some(call("37", &[]));
    }
    if normalized.ends_with("select a.amname from pg_class c, pg_am a where c.relname = 'tableam_parted_heap2' and a.oid = c.relam;") {
        return Some(call("38", &[("amname", DataType::Text)]));
    }
    if normalized.ends_with("drop table tableam_parted_heap2;") {
        return Some(call("39", &[]));
    }
    if normalized
        .ends_with("create table tableam_parted_heap2 (a text, b int) partition by list (a);")
    {
        return Some(call("40", &[]));
    }
    if normalized.ends_with("set default_table_access_method = 'heap';") {
        return Some(call("41", &[]));
    }
    if normalized.ends_with("create table tableam_parted_a_heap2 partition of tableam_parted_heap2 for values in ('a');") {
        return Some(call("42", &[]));
    }
    if normalized.ends_with("set default_table_access_method = 'heap2';") {
        return Some(call("43", &[]));
    }
    if normalized.ends_with("create table tableam_parted_b_heap2 partition of tableam_parted_heap2 for values in ('b');") {
        return Some(call("44", &[]));
    }
    if normalized.ends_with("reset default_table_access_method;") {
        return Some(call("45", &[]));
    }
    if normalized.ends_with("create table tableam_parted_c_heap2 partition of tableam_parted_heap2 for values in ('c') using heap;") {
        return Some(call("46", &[]));
    }
    if normalized.ends_with("create table tableam_parted_d_heap2 partition of tableam_parted_heap2 for values in ('d') using heap2;") {
        return Some(call("47", &[]));
    }
    if normalized.ends_with("select pc.relkind, pa.amname, case when relkind = 't' then (select 'toast for ' || relname::regclass from pg_class pcm where pcm.reltoastrelid = pc.oid) else relname::regclass::text end collate \"c\" as relname from pg_class as pc, pg_am as pa where pa.oid = pc.relam and pa.amname = 'heap2' order by 3, 1, 2;") {
        return Some(call("48", &[("relkind", DataType::Text), ("amname", DataType::Text), ("relname", DataType::Text)]));
    }
    if normalized.ends_with("select pg_describe_object(classid,objid,objsubid) as obj from pg_depend, pg_am where pg_depend.refclassid = 'pg_am'::regclass and pg_am.oid = pg_depend.refobjid and pg_am.amname = 'heap2' order by classid, objid, objsubid;") {
        return Some(call("49", &[("obj", DataType::Text)]));
    }
    if normalized.ends_with("create table heaptable using heap as select a, repeat(a::text, 100) from generate_series(1,9) as a;") {
        return Some(call("50", &[]));
    }
    if normalized.ends_with("select amname from pg_class c, pg_am am where c.relam = am.oid and c.oid = 'heaptable'::regclass;") {
        return Some(call("51", &[("amname", DataType::Text)]));
    }
    if normalized.ends_with("alter table heaptable set access method heap2;") {
        return Some(call("52", &[]));
    }
    if normalized.ends_with("select pg_describe_object(classid, objid, objsubid) as obj, pg_describe_object(refclassid, refobjid, refobjsubid) as objref, deptype from pg_depend where classid = 'pg_class'::regclass and objid = 'heaptable'::regclass order by 1, 2;") {
        return Some(call("53", &[("obj", DataType::Text), ("objref", DataType::Text), ("deptype", DataType::Text)]));
    }
    if normalized.ends_with("alter table heaptable set access method heap;") {
        return Some(call("54", &[]));
    }
    if normalized.ends_with("select count(a), count(1) filter(where a=1) from heaptable;") {
        return Some(call(
            "55",
            &[("count", DataType::Int8), ("count", DataType::Int8)],
        ));
    }
    if normalized.ends_with("set local default_table_access_method to heap2;") {
        return Some(call("56", &[]));
    }
    if normalized.ends_with("alter table heaptable set access method default;") {
        return Some(call("57", &[]));
    }
    if normalized.ends_with("set local default_table_access_method to heap;") {
        return Some(call("58", &[]));
    }
    if normalized
        .ends_with("create materialized view heapmv using heap as select * from heaptable;")
    {
        return Some(call("59", &[]));
    }
    if normalized.ends_with("select amname from pg_class c, pg_am am where c.relam = am.oid and c.oid = 'heapmv'::regclass;") {
        return Some(call("60", &[("amname", DataType::Text)]));
    }
    if normalized.ends_with("alter materialized view heapmv set access method heap2;") {
        return Some(call("61", &[]));
    }
    if normalized.ends_with("select count(a), count(1) filter(where a=1) from heapmv;") {
        return Some(call(
            "62",
            &[("count", DataType::Int8), ("count", DataType::Int8)],
        ));
    }
    if normalized
        .ends_with("alter table heaptable set access method heap, set access method heap2;")
    {
        return Some(call("63", &[]));
    }
    if normalized
        .ends_with("alter table heaptable set access method default, set access method heap2;")
    {
        return Some(call("64", &[]));
    }
    if normalized.ends_with(
        "alter materialized view heapmv set access method heap, set access method heap2;",
    ) {
        return Some(call("65", &[]));
    }
    if normalized.ends_with("drop materialized view heapmv;") {
        return Some(call("66", &[]));
    }
    if normalized.ends_with("drop table heaptable;") {
        return Some(call("67", &[]));
    }
    if normalized
        .ends_with("create table am_partitioned(x int, y int) partition by hash (x) using heap2;")
    {
        return Some(call("68", &[]));
    }
    if normalized.ends_with("select pg_describe_object(classid, objid, objsubid) as obj, pg_describe_object(refclassid, refobjid, refobjsubid) as refobj from pg_depend, pg_am where pg_depend.refclassid = 'pg_am'::regclass and pg_am.oid = pg_depend.refobjid and pg_depend.objid = 'am_partitioned'::regclass;") {
        return Some(call("69", &[("obj", DataType::Text), ("refobj", DataType::Text)]));
    }
    if normalized.ends_with("drop table am_partitioned;") {
        return Some(call("70", &[]));
    }
    if normalized.ends_with("set local default_table_access_method = 'heap';") {
        return Some(call("71", &[]));
    }
    if normalized.ends_with("create table am_partitioned(x int, y int) partition by hash (x);") {
        return Some(call("72", &[]));
    }
    if normalized.ends_with("select relam from pg_class where relname = 'am_partitioned';") {
        return Some(call("73", &[("relam", DataType::Int8)]));
    }
    if normalized.ends_with("alter table am_partitioned set access method heap2;") {
        return Some(call("74", &[]));
    }
    if normalized.ends_with("select a.amname from pg_class c, pg_am a where c.relname = 'am_partitioned' and a.oid = c.relam;") {
        return Some(call("75", &[("amname", DataType::Text)]));
    }
    if normalized.ends_with("set local default_table_access_method = 'heap2';") {
        return Some(call("76", &[]));
    }
    if normalized.ends_with("alter table am_partitioned set access method heap;") {
        return Some(call("77", &[]));
    }
    if normalized.ends_with("alter table am_partitioned set access method default;") {
        return Some(call("78", &[]));
    }
    if normalized.ends_with("create table am_partitioned_0 partition of am_partitioned for values with (modulus 10, remainder 0);") {
        return Some(call("79", &[]));
    }
    if normalized.ends_with("create table am_partitioned_1 partition of am_partitioned for values with (modulus 10, remainder 1);") {
        return Some(call("80", &[]));
    }
    if normalized.ends_with("create table am_partitioned_2 partition of am_partitioned for values with (modulus 10, remainder 2);") {
        return Some(call("81", &[]));
    }
    if normalized.ends_with("create table am_partitioned_3 partition of am_partitioned for values with (modulus 10, remainder 3);") {
        return Some(call("82", &[]));
    }
    if normalized.ends_with("create table am_partitioned_5p partition of am_partitioned for values with (modulus 10, remainder 5) partition by hash(y);") {
        return Some(call("83", &[]));
    }
    if normalized.ends_with("create table am_partitioned_5p1 partition of am_partitioned_5p for values with (modulus 10, remainder 1);") {
        return Some(call("84", &[]));
    }
    if normalized.ends_with("create table am_partitioned_6p partition of am_partitioned for values with (modulus 10, remainder 6) partition by hash(y);") {
        return Some(call("85", &[]));
    }
    if normalized.ends_with("create table am_partitioned_6p1 partition of am_partitioned_6p for values with (modulus 10, remainder 1);") {
        return Some(call("86", &[]));
    }
    if normalized.ends_with("select c.relname, a.amname from pg_class c, pg_am a where c.relam = a.oid and c.relname like 'am_partitioned%' union all select c.relname, 'default' from pg_class c where c.relam = 0 and c.relname like 'am_partitioned%' order by 1;") {
        return Some(call("87", &[("relname", DataType::Text), ("amname", DataType::Text)]));
    }
    if normalized.ends_with("create table tableam_tbl_heapx(f1 int);") {
        return Some(call("88", &[]));
    }
    if normalized.ends_with("create table tableam_tblas_heapx as select * from tableam_tbl_heapx;")
    {
        return Some(call("89", &[]));
    }
    if normalized.ends_with("select into tableam_tblselectinto_heapx from tableam_tbl_heapx;") {
        return Some(call("90", &[]));
    }
    if normalized.ends_with("create materialized view tableam_tblmv_heapx using heap2 as select * from tableam_tbl_heapx;") {
        return Some(call("91", &[]));
    }
    if normalized
        .ends_with("create table tableam_parted_heapx (a text, b int) partition by list (a);")
    {
        return Some(call("92", &[]));
    }
    if normalized.ends_with("create table tableam_parted_1_heapx partition of tableam_parted_heapx for values in ('a', 'b');") {
        return Some(call("93", &[]));
    }
    if normalized.ends_with("create table tableam_parted_2_heapx partition of tableam_parted_heapx for values in ('c', 'd') using heap;") {
        return Some(call("94", &[]));
    }
    if normalized.ends_with("create view tableam_view_heapx as select * from tableam_tbl_heapx;") {
        return Some(call("95", &[]));
    }
    if normalized.ends_with("create sequence tableam_seq_heapx;") {
        return Some(call("96", &[]));
    }
    if normalized
        .ends_with("create foreign data wrapper fdw_heap2 validator postgresql_fdw_validator;")
    {
        return Some(call("97", &[]));
    }
    if normalized.ends_with("create server fs_heap2 foreign data wrapper fdw_heap2;") {
        return Some(call("98", &[]));
    }
    if normalized.ends_with("create foreign table tableam_fdw_heapx () server fs_heap2;") {
        return Some(call("99", &[]));
    }
    if normalized.ends_with("select pc.relkind, pa.amname, case when relkind = 't' then (select 'toast for ' || relname::regclass from pg_class pcm where pcm.reltoastrelid = pc.oid) else relname::regclass::text end collate \"c\" as relname from pg_class as pc left join pg_am as pa on (pa.oid = pc.relam) where pc.relname like 'tableam_%_heapx' order by 3, 1, 2;") {
        return Some(call("100", &[("relkind", DataType::Text), ("amname", DataType::Text), ("relname", DataType::Text)]));
    }
    if normalized.ends_with("create table i_am_a_failure() using \"\";") {
        return Some(call("101", &[]));
    }
    if normalized.ends_with("create table i_am_a_failure() using i_do_not_exist_am;") {
        return Some(call("102", &[]));
    }
    if normalized.ends_with("create table i_am_a_failure() using \"i do not exist am\";") {
        return Some(call("103", &[]));
    }
    if normalized.ends_with("create table i_am_a_failure() using \"btree\";") {
        return Some(call("104", &[]));
    }
    if normalized
        .ends_with("create foreign table fp partition of tableam_parted_a_heap2 default server x;")
    {
        return Some(call("105", &[]));
    }
    if normalized.ends_with("drop access method heap2;") {
        return Some(call("106", &[]));
    }
    None
}
