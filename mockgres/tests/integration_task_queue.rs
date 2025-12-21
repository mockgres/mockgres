mod common;

use tokio_postgres::Client;

#[tokio::test(flavor = "multi_thread")]
async fn test_task_queue_mvp() {
    let ctx = common::start().await;
    let c = &ctx.client;

    c.batch_execute(
        "create table tasks(
            id int primary key,
            payload text not null,
            status text not null default 'pending',
            locked_by text,
            locked_at timestamptz,
            attempts int not null default 0,
            created_at timestamptz default now()
        )",
    )
    .await
    .unwrap();
    c.batch_execute("create index idx_tasks_status on tasks(status)")
        .await
        .unwrap();

    // enqueue tasks (dedup via upsert style)
    for i in 1..=20 {
        c.execute(
            "insert into tasks(id, payload) values ($1, $2)
             on conflict(id) do update set payload = excluded.payload",
            &[
                &i,
                &format!(
                    r#"{{
                "task": {}
            }}"#,
                    i
                ),
            ],
        )
        .await
        .unwrap();
    }
    let pending: i64 = c
        .query_one("select count(*) from tasks where status = 'pending'", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(pending, 20);

    // worker connections
    let w1 = ctx.new_client().await;
    let w2 = ctx.new_client().await;
    let w3 = ctx.new_client().await;

    // helper to pull one task
    async fn pull_one(worker: &mut Client, worker_name: &str) -> i32 {
        let txn = worker.transaction().await.unwrap();
        let row = txn
            .query_one(
                "select id, payload from tasks
                 where status = 'pending'
                 for update skip locked
                 limit 1",
                &[],
            )
            .await
            .unwrap();
        let id: i32 = row.get(0);
        txn.execute(
            "update tasks
             set status = 'processing',
                 locked_by = $1,
                 locked_at = now(),
                 attempts = attempts + 1
             where id = $2",
            &[&worker_name, &id],
        )
        .await
        .unwrap();
        txn.commit().await.unwrap();
        id
    }

    let mut w1 = w1;
    let mut w2 = w2;
    let mut w3 = w3;
    let t1 = pull_one(&mut w1, "worker-1").await;
    let t2 = pull_one(&mut w2, "worker-2").await;
    let t3 = pull_one(&mut w3, "worker-3").await;
    assert!(t1 != t2 && t2 != t3 && t1 != t3);

    let processing: i64 = c
        .query_one(
            "select count(*) from tasks where status = 'processing'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(processing, 3);

    // long running lock on a pending task
    let mut locker = ctx.new_client().await;
    let locker_txn = locker.transaction().await.unwrap();
    let locked_row = locker_txn
        .query_one(
            "select id from tasks where status = 'pending' for update skip locked limit 1",
            &[],
        )
        .await
        .unwrap();
    let locked_id: i32 = locked_row.get(0);

    // worker1 should not see locked id
    let probe_txn = w1.transaction().await.unwrap();
    let probe = probe_txn
        .query_one(
            "select id from tasks where status = 'pending' for update skip locked limit 1",
            &[],
        )
        .await
        .unwrap();
    let probed_id: i32 = probe.get(0);
    assert_ne!(probed_id, locked_id);
    probe_txn.rollback().await.unwrap();
    locker_txn.rollback().await.unwrap();

    // complete and fail tasks
    c.execute("update tasks set status = 'done' where id = $1", &[&t1])
        .await
        .unwrap();
    c.execute(
        "update tasks set status = 'pending', locked_by = null, locked_at = null where id = $1",
        &[&t3],
    )
    .await
    .unwrap();

    let status_rows = c
        .query(
            "select id, status from tasks where id in ($1,$2,$3) order by id",
            &[&t1, &t2, &t3],
        )
        .await
        .unwrap();
    let statuses: Vec<String> = status_rows.into_iter().map(|r| r.get(1)).collect();
    assert_eq!(statuses.len(), 3);
    assert!(statuses.contains(&"done".to_string()));
    assert!(statuses.contains(&"pending".to_string()));
    assert!(statuses.contains(&"processing".to_string()));

    // try again failed task (t3)
    let repull = c
        .query_one(
            "select id from tasks where status = 'pending' and id = $1 for update skip locked",
            &[&t3],
        )
        .await
        .unwrap();
    let repull_id: i32 = repull.get(0);
    assert_eq!(repull_id, t3);

    // cleanup
    c.execute(
        "delete from tasks where status = 'done' and created_at < now() + interval '1 second'",
        &[],
    )
    .await
    .unwrap();
    let done_left: i64 = c
        .query_one("select count(*) from tasks where status = 'done'", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(done_left, 0);

    let _ = ctx.shutdown.send(());
}
