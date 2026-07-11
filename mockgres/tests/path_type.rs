mod common;

use std::error::Error;
use std::io::{Error as IoError, ErrorKind};

use bytes::{BufMut, BytesMut};
use postgres_types::{FromSql, IsNull, ToSql, Type};
use tokio_postgres::SimpleQueryMessage;
use tokio_postgres::error::SqlState;

#[derive(Clone, Debug, PartialEq)]
struct PgPath {
    closed: bool,
    points: Vec<(f64, f64)>,
}

impl PgPath {
    fn open(points: Vec<(f64, f64)>) -> Self {
        Self {
            closed: false,
            points,
        }
    }

    fn closed(points: Vec<(f64, f64)>) -> Self {
        Self {
            closed: true,
            points,
        }
    }
}

impl<'a> FromSql<'a> for PgPath {
    fn from_sql(_ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        if raw.len() < 5 {
            return Err(IoError::new(ErrorKind::InvalidData, "path header is truncated").into());
        }
        let closed = match raw[0] {
            0 => false,
            1 => true,
            _ => {
                return Err(
                    IoError::new(ErrorKind::InvalidData, "path closed flag is invalid").into(),
                );
            }
        };
        let point_count = i32::from_be_bytes(raw[1..5].try_into()?) as usize;
        if point_count == 0 || raw.len() != 5 + point_count * 16 {
            return Err(IoError::new(ErrorKind::InvalidData, "path length is invalid").into());
        }
        let points = raw[5..]
            .chunks_exact(16)
            .map(|point| {
                let x = f64::from_be_bytes(point[..8].try_into().expect("x coordinate width"));
                let y = f64::from_be_bytes(point[8..].try_into().expect("y coordinate width"));
                (x, y)
            })
            .collect();
        Ok(Self { closed, points })
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::PATH
    }
}

impl ToSql for PgPath {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.put_u8(u8::from(self.closed));
        out.put_i32(self.points.len() as i32);
        for (x, y) in &self.points {
            out.put_f64(*x);
            out.put_f64(*y);
        }
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::PATH
    }

    postgres_types::to_sql_checked!();
}

fn assert_sqlstate(error: &tokio_postgres::Error, expected: &SqlState) {
    let db_error = error.as_db_error().expect("expected database error");
    assert_eq!(db_error.code(), expected, "unexpected error: {db_error:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn path_input_output_and_binary_wire_format_match_postgres() {
    let ctx = common::start().await;
    ctx.client
        .execute("create table paths (id int4 primary key, value path)", &[])
        .await
        .expect("create path table");
    ctx.client
        .execute(
            "insert into paths values \
             (1, '[(1,2),(3,4)]'), \
             (2, ' ( ( 1 , 2 ) , ( 3 , 4 ) ) '), \
             (3, '[ (0,0),(3,0),(4,5),(1,6) ]'), \
             (4, '1,2 ,3,4 '), \
             (5, ' [1,2,3, 4] '), \
             (6, '((10,20))'), \
             (7, '[ 11,12,13,14 ]'), \
             (8, '( 11,12,13,14)'), \
             (9, '[ ( NaN , Infinity ) ]'), \
             (10, '[(1,2),3,4]'), \
             (11, '[1,2,(3,4)]')",
            &[],
        )
        .await
        .expect("insert path values");

    let messages = ctx
        .client
        .simple_query("select value from paths order by id")
        .await
        .expect("select path text output");
    let values: Vec<String> = messages
        .iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => row.get(0).map(str::to_string),
            _ => None,
        })
        .collect();
    assert_eq!(
        values,
        [
            "[(1,2),(3,4)]",
            "((1,2),(3,4))",
            "[(0,0),(3,0),(4,5),(1,6)]",
            "((1,2),(3,4))",
            "[(1,2),(3,4)]",
            "((10,20))",
            "[(11,12),(13,14)]",
            "((11,12),(13,14))",
            "[(NaN,Infinity)]",
            "[(1,2),(3,4)]",
            "[(1,2),(3,4)]",
        ]
    );

    let row = ctx
        .client
        .query_one("select '((1.25,-2.5),(3,4))'::path", &[])
        .await
        .expect("select binary path");
    assert_eq!(row.columns()[0].type_(), &Type::PATH);
    assert_eq!(
        row.get::<_, PgPath>(0),
        PgPath::closed(vec![(1.25, -2.5), (3.0, 4.0)])
    );

    let parameter = PgPath::open(vec![(7.5, -8.25), (9.0, 10.0)]);
    ctx.client
        .execute("insert into paths values ($1, $2)", &[&12_i32, &parameter])
        .await
        .expect("insert binary path parameter");
    let round_trip: PgPath = ctx
        .client
        .query_one("select value from paths where id = 12", &[])
        .await
        .expect("select binary path parameter")
        .get(0);
    assert_eq!(round_trip, parameter);

    let text: String = ctx
        .client
        .query_one("select '[1.25,-2.5,3,4]'::path::text", &[])
        .await
        .expect("cast path to text")
        .get(0);
    assert_eq!(text, "[(1.25,-2.5),(3,4)]");

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn path_rejects_malformed_and_out_of_range_coordinates() {
    let ctx = common::start().await;
    ctx.client
        .execute("create table paths (value path)", &[])
        .await
        .expect("create path table");

    for input in [
        "[]",
        "[()]",
        "[(,2),(3,4)]",
        "[(1,2),(3,4)",
        "(1,2,3,4",
        "(1,2),(3,4)]",
        "[(1,2),(3)]",
        "[(1,2,6),(3,4,6)]",
        "[(1,2) (3,4)]",
        "[(1,2), (3,4),]",
        "1,2,",
    ] {
        let sql = format!("insert into paths values ('{input}')");
        let error = ctx
            .client
            .execute(&sql, &[])
            .await
            .expect_err("malformed path should fail");
        assert_sqlstate(&error, &SqlState::INVALID_TEXT_REPRESENTATION);
    }

    let error = ctx
        .client
        .execute("insert into paths values ('[(1e500,2)]')", &[])
        .await
        .expect_err("out-of-range path coordinate should fail");
    assert_sqlstate(&error, &SqlState::NUMERIC_VALUE_OUT_OF_RANGE);

    let _ = ctx.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn postgres_streets_fixture_copies_into_a_path_column() {
    let ctx = common::start().await;
    ctx.client
        .execute("create table road (name text, thepath path)", &[])
        .await
        .expect("create road table");

    let fixture = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../vendor/postgres-18.4/regress/data/streets.data")
        .canonicalize()
        .expect("locate PostgreSQL streets fixture");
    let contents = std::fs::read_to_string(&fixture).expect("read PostgreSQL streets fixture");
    let expected_rows = contents.lines().count() as u64;
    let fixture = fixture
        .to_str()
        .expect("fixture path is UTF-8")
        .replace('\'', "''");
    let copied = ctx
        .client
        .execute(&format!("copy road from '{fixture}'"), &[])
        .await
        .expect("copy PostgreSQL streets fixture");
    assert_eq!(copied, expected_rows);

    let row = ctx
        .client
        .query_one("select name, thepath from road limit 1", &[])
        .await
        .expect("select copied street");
    assert!(!row.get::<_, String>(0).is_empty());
    let path = row.get::<_, PgPath>(1);
    assert!(!path.closed);
    assert!(path.points.len() >= 2);

    let _ = ctx.shutdown.send(());
}
