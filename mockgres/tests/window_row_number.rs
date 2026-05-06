mod common;

#[tokio::test(flavor = "multi_thread")]
async fn row_number_partition_order_filters_duplicates() {
    let ctx = common::start().await;

    ctx.client
        .batch_execute(
            "
            CREATE TABLE foo (
                id BIGINT PRIMARY KEY,
                bar INT NOT NULL,
                baz INT NOT NULL
            );

            INSERT INTO foo (id, bar, baz) VALUES
                (1, 10, 100),
                (2, 10, 100),
                (3, 10, 100),
                (4, 10, 200),
                (5, 20, 100),
                (6, 20, 100);
            ",
        )
        .await
        .expect("setup foo");

    let rows = ctx
        .client
        .query(
            "
            SELECT id
            FROM (
                SELECT
                    id,
                    row_number() OVER (
                        PARTITION BY bar, baz
                        ORDER BY id DESC
                    ) AS duplicate_rank
                FROM foo
            ) ranked
            WHERE duplicate_rank > 1
            ORDER BY id;
            ",
            &[],
        )
        .await
        .expect("select duplicate ids");

    let ids: Vec<i64> = rows.iter().map(|row| row.get(0)).collect();
    assert_eq!(ids, vec![1, 2, 5]);

    let _ = ctx.shutdown.send(());
}
