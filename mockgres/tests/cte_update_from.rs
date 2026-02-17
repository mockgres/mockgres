mod common;

#[tokio::test(flavor = "multi_thread")]
async fn cte_update_from_updates_expected_rows() {
    let ctx = common::start().await;
    ctx.client
        .batch_execute(
            "create table tasks(id int primary key, status text not null);
             insert into tasks values (1, 'pending'), (2, 'pending'), (3, 'done')",
        )
        .await
        .unwrap();

    let rows = ctx
        .client
        .query(
            "with picked as (
                select id from tasks where status = 'pending' order by id limit 1
             )
             update tasks
                set status = 'processing'
               from picked
              where tasks.id = picked.id
            returning tasks.id, tasks.status",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let id: i32 = rows[0].get(0);
    let status: String = rows[0].get(1);
    assert_eq!((id, status.as_str()), (1, "processing"));

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn cte_for_update_skip_locked_is_preserved() {
    let ctx = common::start().await;
    ctx.client
        .batch_execute(
            "create table tasks(id int primary key, status text not null);
             insert into tasks values (1, 'pending'), (2, 'pending')",
        )
        .await
        .unwrap();

    let locker = ctx.new_client().await;
    locker.batch_execute("begin").await.unwrap();
    let locked = locker
        .query_one(
            "with picked as (
                select id
                  from tasks
                 where status = 'pending'
                 order by id
                 for update skip locked
                 limit 1
             )
             select id from picked",
            &[],
        )
        .await
        .unwrap();
    let locked_id: i32 = locked.get(0);

    let worker = ctx.new_client().await;
    let updated = worker
        .query_one(
            "with picked as (
                select id
                  from tasks
                 where status = 'pending'
                 order by id
                 for update skip locked
                 limit 1
             )
             update tasks
                set status = 'processing'
               from picked
              where tasks.id = picked.id
            returning tasks.id",
            &[],
        )
        .await
        .unwrap();
    let updated_id: i32 = updated.get(0);
    assert_ne!(updated_id, locked_id);

    locker.batch_execute("rollback").await.unwrap();
    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn cte_update_with_target_alias_and_qualified_columns_is_supported() {
    let ctx = common::start().await;
    ctx.client
        .batch_execute(
            "create table queue_jobs(
                id int primary key,
                kind text not null,
                payload text not null,
                priority int not null,
                status text not null,
                run_at int not null,
                attempts int not null,
                max_attempts int not null,
                last_error text,
                lease_expires_at int,
                created_at int not null,
                updated_at int not null
             );
             insert into queue_jobs values
               (1, 'email', '{}', 1, 'pending', 10, 2, 5, null, null, 1, 1),
               (2, 'email', '{}', 2, 'pending', 11, 0, 5, null, null, 1, 1)",
        )
        .await
        .unwrap();

    let row = ctx
        .client
        .query_one(
            "with picked as (
                select id
                from queue_jobs
                where status = $1
                  and run_at <= $2
                order by priority asc, run_at asc, id asc
                for update skip locked
                limit $3
            )
            update queue_jobs q
            set status = $4,
                attempts = q.attempts + 1,
                lease_expires_at = $5,
                updated_at = $2
            from picked
            where q.id = picked.id
            returning
                q.id,
                q.kind,
                q.payload,
                q.priority,
                q.status,
                q.run_at,
                q.attempts,
                q.max_attempts,
                coalesce(q.last_error, ''),
                q.lease_expires_at,
                q.created_at,
                q.updated_at",
            &[&"pending", &10_i32, &1_i64, &"running", &99_i32],
        )
        .await
        .unwrap();
    let id: i32 = row.get(0);
    let status: String = row.get(4);
    let attempts: i32 = row.get(6);
    let lease_expires_at: i32 = row.get(9);
    assert_eq!(id, 1);
    assert_eq!(status, "running");
    assert_eq!(attempts, 3);
    assert_eq!(lease_expires_at, 99);

    let _ = ctx.shutdown.send(());
}
