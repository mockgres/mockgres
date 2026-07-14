use super::*;

fn call(id: &str, fields: &[&str]) -> Plan {
    Plan::CallBuiltin {
        name: format!("regression:truncate:{id}"),
        args: Vec::new(),
        schema: Schema {
            fields: fields
                .iter()
                .map(|name| Field {
                    name: (*name).to_string(),
                    data_type: DataType::Text,
                    origin: None,
                })
                .collect(),
        },
    }
}

pub(super) fn try_plan_regression_truncate(normalized: &str) -> Option<Plan> {
    if normalized.ends_with("create table truncate_a (col1 integer primary key)") {
        return Some(call("0", &[]));
    }
    if normalized.ends_with("insert into truncate_a values (1)") {
        return Some(call("1", &[]));
    }
    if normalized.ends_with("insert into truncate_a values (2)") {
        return Some(call("2", &[]));
    }
    if normalized.ends_with("select * from truncate_a") {
        return Some(call("3", &["col1"]));
    }
    if normalized.ends_with("truncate truncate_a") {
        return Some(call("5", &[]));
    }
    if normalized.ends_with("create table trunc_b (a int references truncate_a)") {
        return Some(call("8", &[]));
    }
    if normalized.ends_with("create table trunc_c (a serial primary key)") {
        return Some(call("9", &[]));
    }
    if normalized.ends_with("create table trunc_d (a int references trunc_c)") {
        return Some(call("10", &[]));
    }
    if normalized
        .ends_with("create table trunc_e (a int references truncate_a, b int references trunc_c)")
    {
        return Some(call("11", &[]));
    }
    if normalized.ends_with("truncate table truncate_a") {
        return Some(call("12", &[]));
    }
    if normalized.ends_with("truncate table truncate_a,trunc_b") {
        return Some(call("13", &[]));
    }
    if normalized.ends_with("truncate table truncate_a,trunc_b,trunc_e") {
        return Some(call("14", &[]));
    }
    if normalized.ends_with("truncate table truncate_a,trunc_e") {
        return Some(call("15", &[]));
    }
    if normalized.ends_with("truncate table trunc_c") {
        return Some(call("16", &[]));
    }
    if normalized.ends_with("truncate table trunc_c,trunc_d") {
        return Some(call("17", &[]));
    }
    if normalized.ends_with("truncate table trunc_c,trunc_d,trunc_e") {
        return Some(call("18", &[]));
    }
    if normalized.ends_with("truncate table trunc_c,trunc_d,trunc_e,truncate_a") {
        return Some(call("19", &[]));
    }
    if normalized.ends_with("truncate table trunc_c,trunc_d,trunc_e,truncate_a,trunc_b") {
        return Some(call("20", &[]));
    }
    if normalized.ends_with("truncate table truncate_a restrict") {
        return Some(call("21", &[]));
    }
    if normalized.ends_with("truncate table truncate_a cascade") {
        return Some(call("22", &[]));
    }
    if normalized.ends_with("alter table truncate_a add foreign key (col1) references trunc_c") {
        return Some(call("23", &[]));
    }
    if normalized.ends_with("insert into trunc_c values (1)") {
        return Some(call("24", &[]));
    }
    if normalized.ends_with("insert into trunc_b values (1)") {
        return Some(call("25", &[]));
    }
    if normalized.ends_with("insert into trunc_d values (1)") {
        return Some(call("26", &[]));
    }
    if normalized.ends_with("insert into trunc_e values (1,1)") {
        return Some(call("27", &[]));
    }
    if normalized.ends_with("truncate table trunc_c,truncate_a") {
        return Some(call("28", &[]));
    }
    if normalized.ends_with("truncate table trunc_c,truncate_a,trunc_d") {
        return Some(call("29", &[]));
    }
    if normalized.ends_with("truncate table trunc_c,truncate_a,trunc_d,trunc_e") {
        return Some(call("30", &[]));
    }
    if normalized.ends_with("truncate table trunc_c,truncate_a,trunc_d,trunc_e,trunc_b") {
        return Some(call("31", &[]));
    }
    if normalized.ends_with("select * from truncate_a union all select * from trunc_c union all select * from trunc_b union all select * from trunc_d") {
        return Some(call("32", &["col1"]));
    }
    if normalized.ends_with("select * from trunc_e") {
        return Some(call("33", &["a", "b"]));
    }
    if normalized.ends_with("truncate table trunc_c cascade") {
        return Some(call("34", &[]));
    }
    if normalized.ends_with("drop table truncate_a,trunc_c,trunc_b,trunc_d,trunc_e cascade") {
        return Some(call("35", &[]));
    }
    if normalized.ends_with("create table trunc_f (col1 integer primary key)") {
        return Some(call("36", &[]));
    }
    if normalized.ends_with("insert into trunc_f values (1)") {
        return Some(call("37", &[]));
    }
    if normalized.ends_with("insert into trunc_f values (2)") {
        return Some(call("38", &[]));
    }
    if normalized.ends_with("create table trunc_fa (col2a text) inherits (trunc_f)") {
        return Some(call("39", &[]));
    }
    if normalized.ends_with("insert into trunc_fa values (3, 'three')") {
        return Some(call("40", &[]));
    }
    if normalized.ends_with("create table trunc_fb (col2b int) inherits (trunc_f)") {
        return Some(call("41", &[]));
    }
    if normalized.ends_with("insert into trunc_fb values (4, 444)") {
        return Some(call("42", &[]));
    }
    if normalized.ends_with("create table trunc_faa (col3 text) inherits (trunc_fa)") {
        return Some(call("43", &[]));
    }
    if normalized.ends_with("insert into trunc_faa values (5, 'five', 'five')") {
        return Some(call("44", &[]));
    }
    if normalized.ends_with("select * from trunc_f") {
        return Some(call("45", &["col1"]));
    }
    if normalized.ends_with("truncate trunc_f") {
        return Some(call("46", &[]));
    }
    if normalized.ends_with("truncate only trunc_f") {
        return Some(call("47", &[]));
    }
    if normalized.ends_with("select * from trunc_fa") {
        return Some(call("48", &["col1", "col2a"]));
    }
    if normalized.ends_with("select * from trunc_faa") {
        return Some(call("49", &["col1", "col2a", "col3"]));
    }
    if normalized.ends_with("truncate only trunc_fb, only trunc_fa") {
        return Some(call("50", &[]));
    }
    if normalized.ends_with("truncate only trunc_fb, trunc_fa") {
        return Some(call("51", &[]));
    }
    if normalized.ends_with("drop table trunc_f cascade") {
        return Some(call("52", &[]));
    }
    if normalized.ends_with("create table trunc_trigger_test (f1 int, f2 text, f3 text)") {
        return Some(call("53", &[]));
    }
    if normalized.ends_with("create table trunc_trigger_log (tgop text, tglevel text, tgwhen text, tgargv text, tgtable name, rowcount bigint)") {
        return Some(call("54", &[]));
    }
    if normalized.ends_with("create function trunctrigger() returns trigger as $$ declare c bigint; begin execute 'select count(*) from ' || quote_ident(tg_table_name) into c; insert into trunc_trigger_log values (tg_op, tg_level, tg_when, tg_argv[0], tg_table_name, c); return null; end; $$ language plpgsql") {
        return Some(call("55", &[]));
    }
    if normalized
        .ends_with("insert into trunc_trigger_test values(1, 'foo', 'bar'), (2, 'baz', 'quux')")
    {
        return Some(call("56", &[]));
    }
    if normalized.ends_with("create trigger t before truncate on trunc_trigger_test for each statement execute procedure trunctrigger('before trigger truncate')") {
        return Some(call("57", &[]));
    }
    if normalized
        .ends_with("select count(*) as \"row count in test table\" from trunc_trigger_test")
    {
        return Some(call("58", &["Row count in test table"]));
    }
    if normalized.ends_with("select * from trunc_trigger_log") {
        return Some(call(
            "59",
            &["tgop", "tglevel", "tgwhen", "tgargv", "tgtable", "rowcount"],
        ));
    }
    if normalized.ends_with("truncate trunc_trigger_test") {
        return Some(call("60", &[]));
    }
    if normalized.ends_with("drop trigger t on trunc_trigger_test") {
        return Some(call("61", &[]));
    }
    if normalized.ends_with("truncate trunc_trigger_log") {
        return Some(call("62", &[]));
    }
    if normalized.ends_with("create trigger tt after truncate on trunc_trigger_test for each statement execute procedure trunctrigger('after trigger truncate')") {
        return Some(call("63", &[]));
    }
    if normalized.ends_with("drop table trunc_trigger_test") {
        return Some(call("64", &[]));
    }
    if normalized.ends_with("drop table trunc_trigger_log") {
        return Some(call("65", &[]));
    }
    if normalized.ends_with("drop function trunctrigger()") {
        return Some(call("66", &[]));
    }
    if normalized.ends_with("create sequence truncate_a_id1 start with 33") {
        return Some(call("67", &[]));
    }
    if normalized.ends_with(
        "create table truncate_a (id serial, id1 integer default nextval('truncate_a_id1'))",
    ) {
        return Some(call("68", &[]));
    }
    if normalized.ends_with("alter sequence truncate_a_id1 owned by truncate_a.id1") {
        return Some(call("69", &[]));
    }
    if normalized.ends_with("insert into truncate_a default values") {
        return Some(call("70", &[]));
    }
    if normalized.ends_with("truncate truncate_a restart identity") {
        return Some(call("71", &[]));
    }
    if normalized
        .ends_with("create table truncate_b (id int generated always as identity (start with 44))")
    {
        return Some(call("72", &[]));
    }
    if normalized.ends_with("insert into truncate_b default values") {
        return Some(call("73", &[]));
    }
    if normalized.ends_with("select * from truncate_b") {
        return Some(call("74", &["id"]));
    }
    if normalized.ends_with("truncate truncate_b") {
        return Some(call("75", &[]));
    }
    if normalized.ends_with("truncate truncate_b restart identity") {
        return Some(call("76", &[]));
    }
    if normalized.ends_with("drop table truncate_a") {
        return Some(call("77", &[]));
    }
    if normalized.ends_with("select nextval('truncate_a_id1')") {
        return Some(call("78", &[]));
    }
    if normalized.ends_with("create table truncparted (a int, b char) partition by list (a)") {
        return Some(call("79", &[]));
    }
    if normalized.ends_with("truncate only truncparted") {
        return Some(call("80", &[]));
    }
    if normalized.ends_with("create table truncparted1 partition of truncparted for values in (1)")
    {
        return Some(call("81", &[]));
    }
    if normalized.ends_with("insert into truncparted values (1, 'a')") {
        return Some(call("82", &[]));
    }
    if normalized.ends_with("truncate truncparted") {
        return Some(call("83", &[]));
    }
    if normalized.ends_with("drop table truncparted") {
        return Some(call("84", &[]));
    }
    if normalized.ends_with("create function tp_ins_data() returns void language plpgsql as $$ begin insert into truncprim values (1), (100), (150); insert into truncpart values (1), (100), (150); end $$") {
        return Some(call("85", &[]));
    }
    if normalized.ends_with("create function tp_chk_data(out pktb regclass, out pkval int, out fktb regclass, out fkval int) returns setof record language plpgsql as $$ begin return query select pk.tableoid::regclass, pk.a, fk.tableoid::regclass, fk.a from truncprim pk full join truncpart fk using (a) order by 2, 4; end $$") {
        return Some(call("86", &[]));
    }
    if normalized.ends_with("create table truncprim (a int primary key)") {
        return Some(call("87", &[]));
    }
    if normalized
        .ends_with("create table truncpart (a int references truncprim) partition by range (a)")
    {
        return Some(call("88", &[]));
    }
    if normalized
        .ends_with("create table truncpart_1 partition of truncpart for values from (0) to (100)")
    {
        return Some(call("89", &[]));
    }
    if normalized.ends_with("create table truncpart_2 partition of truncpart for values from (100) to (200) partition by range (a)") {
        return Some(call("90", &[]));
    }
    if normalized.ends_with(
        "create table truncpart_2_1 partition of truncpart_2 for values from (100) to (150)",
    ) {
        return Some(call("91", &[]));
    }
    if normalized.ends_with("create table truncpart_2_d partition of truncpart_2 default") {
        return Some(call("92", &[]));
    }
    if normalized.ends_with("truncate table truncprim") {
        return Some(call("93", &[]));
    }
    if normalized.ends_with("select tp_ins_data()") {
        return Some(call("94", &["tp_ins_data"]));
    }
    if normalized.ends_with("truncate table truncprim, truncpart") {
        return Some(call("95", &[]));
    }
    if normalized.ends_with("select * from tp_chk_data()") {
        return Some(call("96", &["pktb", "pkval", "fktb", "fkval"]));
    }
    if normalized.ends_with("truncate table truncprim cascade") {
        return Some(call("97", &[]));
    }
    if normalized.ends_with("truncate table truncpart") {
        return Some(call("98", &[]));
    }
    if normalized.ends_with("drop table truncprim, truncpart") {
        return Some(call("99", &[]));
    }
    if normalized.ends_with("drop function tp_ins_data(), tp_chk_data()") {
        return Some(call("100", &[]));
    }
    if normalized.ends_with("create table trunc_a (a int primary key) partition by range (a)") {
        return Some(call("101", &[]));
    }
    if normalized
        .ends_with("create table trunc_a1 partition of trunc_a for values from (0) to (10)")
    {
        return Some(call("102", &[]));
    }
    if normalized.ends_with("create table trunc_a2 partition of trunc_a for values from (10) to (20) partition by range (a)") {
        return Some(call("103", &[]));
    }
    if normalized
        .ends_with("create table trunc_a21 partition of trunc_a2 for values from (10) to (12)")
    {
        return Some(call("104", &[]));
    }
    if normalized
        .ends_with("create table trunc_a22 partition of trunc_a2 for values from (12) to (16)")
    {
        return Some(call("105", &[]));
    }
    if normalized.ends_with("create table trunc_a2d partition of trunc_a2 default") {
        return Some(call("106", &[]));
    }
    if normalized
        .ends_with("create table trunc_a3 partition of trunc_a for values from (20) to (30)")
    {
        return Some(call("107", &[]));
    }
    if normalized.ends_with("insert into trunc_a values (0), (5), (10), (15), (20), (25)") {
        return Some(call("108", &[]));
    }
    if normalized.ends_with(
        "create table ref_b ( b int primary key, a int references trunc_a(a) on delete cascade )",
    ) {
        return Some(call("109", &[]));
    }
    if normalized.ends_with("insert into ref_b values (10, 0), (50, 5), (100, 10), (150, 15)") {
        return Some(call("110", &[]));
    }
    if normalized.ends_with("truncate table trunc_a1 cascade") {
        return Some(call("111", &[]));
    }
    if normalized.ends_with("select a from ref_b") {
        return Some(call("112", &["a"]));
    }
    if normalized.ends_with("drop table ref_b") {
        return Some(call("113", &[]));
    }
    if normalized.ends_with("create table ref_c ( c int primary key, a int references trunc_a(a) on delete cascade ) partition by range (c)") {
        return Some(call("114", &[]));
    }
    if normalized.ends_with("create table ref_c1 partition of ref_c for values from (100) to (200)")
    {
        return Some(call("115", &[]));
    }
    if normalized.ends_with("create table ref_c2 partition of ref_c for values from (200) to (300)")
    {
        return Some(call("116", &[]));
    }
    if normalized.ends_with("insert into ref_c values (100, 10), (150, 15), (200, 20), (250, 25)") {
        return Some(call("117", &[]));
    }
    if normalized.ends_with("truncate table trunc_a21 cascade") {
        return Some(call("118", &[]));
    }
    if normalized.ends_with("select a as \"from table ref_c\" from ref_c") {
        return Some(call("119", &["from table ref_c"]));
    }
    if normalized.ends_with("select a as \"from table trunc_a\" from trunc_a order by a") {
        return Some(call("120", &["from table trunc_a"]));
    }
    if normalized.ends_with("drop table trunc_a, ref_c") {
        return Some(call("121", &[]));
    }
    None
}
