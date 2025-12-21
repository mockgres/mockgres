mod common;

#[tokio::test(flavor = "multi_thread")]
async fn drop_fk_by_postgres_default_name() {
    let ctx = common::start().await;

    ctx.client
        .execute("create table partitions(id int primary key)", &[])
        .await
        .expect("create partitions");
    ctx.client
        .execute(
            "create table wal_file_offsets(
                partition_id int not null references partitions(id),
                base_offset int not null
            )",
            &[],
        )
        .await
        .expect("create wal_file_offsets");

    ctx.client
        .execute(
            "alter table wal_file_offsets drop constraint wal_file_offsets_partition_id_fkey",
            &[],
        )
        .await
        .expect("drop foreign key constraint by default name");

    ctx.client
        .execute("alter table wal_file_offsets drop column partition_id", &[])
        .await
        .expect("drop non-last column after dropping FK");

    ctx.client
        .execute("insert into wal_file_offsets values (123)", &[])
        .await
        .expect("insert without parent after dropping FK");

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn alter_table_add_foreign_key_to_unique_constraint() {
    let ctx = common::start().await;

    ctx.client
        .execute(
            "create table partitions(
                topic_name text not null,
                partition_number int not null,
                id int primary key,
                unique(topic_name, partition_number)
            )",
            &[],
        )
        .await
        .expect("create partitions");
    ctx.client
        .execute(
            "create table wal_file_offsets(
                topic_name text not null,
                partition_number int not null
            )",
            &[],
        )
        .await
        .expect("create wal_file_offsets");
    ctx.client
        .execute("insert into partitions values ('topic-a', 0, 1)", &[])
        .await
        .expect("insert partition");
    ctx.client
        .execute("insert into wal_file_offsets values ('topic-a', 0)", &[])
        .await
        .expect("insert child row before FK");

    ctx.client
        .execute(
            "alter table wal_file_offsets add constraint wal_file_offsets_topic_name_partition_number_fkey \
             foreign key (topic_name, partition_number) references partitions(topic_name, partition_number) on delete cascade",
            &[],
        )
        .await
        .expect("add foreign key");

    let err = ctx
        .client
        .execute("insert into wal_file_offsets values ('missing', 0)", &[])
        .await
        .expect_err("insert without parent should fail");
    let db_err = err.as_db_error().expect("expected db error");
    assert!(
        db_err.message().contains("foreign key"),
        "unexpected error: {:?}",
        db_err.message()
    );

    ctx.client
        .execute("insert into wal_file_offsets values ('topic-a', 0)", &[])
        .await
        .expect("insert child row after FK");

    ctx.client
        .execute("delete from partitions where id = 1", &[])
        .await
        .expect("delete parent should cascade");

    let row = ctx
        .client
        .query_one("select count(*) from wal_file_offsets", &[])
        .await
        .expect("count child rows");
    let count: i64 = row.get(0);
    assert_eq!(count, 0);

    let _ = ctx.shutdown.send(());
}
