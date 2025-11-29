mod common;

use tokio_postgres::Row;

#[tokio::test(flavor = "multi_thread")]
async fn test_reddit_mvp() {
    let ctx = common::start().await;
    let c = &ctx.client;

    // schema
    c.batch_execute(
        "create table users(
            id int primary key,
            username text unique not null,
            created_at timestamptz default now()
        )",
    )
    .await
    .unwrap();
    c.batch_execute(
        "create table posts(
            id int primary key,
            user_id int not null references users(id),
            title text not null,
            body text,
            created_at timestamptz default now(),
            hot_score double precision default 0
        )",
    )
    .await
    .unwrap();
    c.batch_execute(
        "create table comments(
            id int primary key,
            post_id int not null references posts(id),
            user_id int not null references users(id),
            body text not null,
            created_at timestamptz default now()
        )",
    )
    .await
    .unwrap();
    c.batch_execute(
        "create table votes(
            user_id int references users(id),
            post_id int references posts(id),
            value int not null,
            created_at timestamptz default now(),
            primary key(user_id, post_id)
        )",
    )
    .await
    .unwrap();

    // seed users
    let user_rows = c
        .query(
            "insert into users(id, username) values (1,'alice'),(2,'bob'),(3,'charlie') returning id",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(user_rows.len(), 3);

    // seed posts
    let post_rows = c
        .query(
            "insert into posts(id, user_id, title, body) values
             (1,1,'Hello world','My first post'),
             (2,2,'Rust is amazing','Memory safety for free'),
             (3,1,'Check out my cat','She is very fluffy')
             returning id",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(post_rows.len(), 3);

    // seed comments
    let comment_rows = c
        .query(
            "insert into comments(id, post_id, user_id, body) values
             (1,1,2,'Nice post!'),
             (2,1,3,'I agree!'),
             (3,2,1,'Rustaceans unite!')
             returning id",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(comment_rows.len(), 3);

    // voting with upserts
    c.batch_execute(
        "insert into votes(user_id, post_id, value) values
            (1,1,1),
            (2,1,1),
            (3,1,1)
        on conflict(user_id, post_id) do update set value = excluded.value",
    )
    .await
    .unwrap();
    c.batch_execute(
        "insert into votes(user_id, post_id, value) values (1,1,-1)
        on conflict(user_id, post_id) do update set value = excluded.value",
    )
    .await
    .unwrap();
    c.batch_execute(
        "insert into votes(user_id, post_id, value) values
            (1,2,1),
            (2,2,1)
        on conflict(user_id, post_id) do update set value = excluded.value",
    )
    .await
    .unwrap();

    let vote_rows = c
        .query(
            "select user_id, post_id, value from votes order by user_id, post_id",
            &[],
        )
        .await
        .unwrap();
    let vote_triplets: Vec<(i32, i32, i32)> = vote_rows
        .into_iter()
        .map(|r| (r.get(0), r.get(1), r.get(2)))
        .collect();
    assert_eq!(
        vote_triplets,
        vec![(1, 1, -1), (1, 2, 1), (2, 1, 1), (2, 2, 1), (3, 1, 1)]
    );

    // hot score recomputation
    c.batch_execute(
        "
        update posts
        set hot_score = sub.hot
        from (
            select
                agg.post_id,
                log(greatest(abs(agg.vote_sum), 1)) +
                extract(epoch from now()) / 45000 as hot
            from (
                select post_id, sum(value) as vote_sum
                from votes
                group by post_id
            ) agg
        ) sub
        where posts.id = sub.post_id
    ",
    )
    .await
    .unwrap();

    // front page ordering
    let front_page = c
        .query(
            "select p.id, p.title, u.username, p.hot_score
             from posts p
             join users u on p.user_id = u.id
             order by p.hot_score desc, p.id asc",
            &[],
        )
        .await
        .unwrap();
    let ordered_ids: Vec<i32> = front_page.iter().map(|r| r.get(0)).collect();
    assert_eq!(ordered_ids[..3], [2, 1, 3]);

    // fetch post with comments
    let joined = c
        .query(
            "
            select
              p.id, p.title, p.body,
              u.username as author,
              p.hot_score,
              c.id as comment_id,
              cu.username as comment_author,
              c.body as comment_body
            from posts p
            join users u on p.user_id = u.id
            left join comments c on c.post_id = p.id
            left join users cu on cu.id = c.user_id
            where p.id = 1
            order by c.id asc",
            &[],
        )
        .await
        .unwrap();
    assert!(joined.len() >= 2);
    assert_eq!(joined[0].get::<_, i32>(0), 1);
    assert_eq!(joined[0].get::<_, String>(1), "Hello world");
    // comment ordering
    let comment_ids: Vec<Option<i32>> = joined.iter().map(|r: &Row| r.get(5)).collect();
    let mut sorted_ids = comment_ids.clone();
    sorted_ids.sort();
    assert_eq!(sorted_ids, vec![Some(1), Some(2)]);

    let _ = ctx.shutdown.send(());
}
