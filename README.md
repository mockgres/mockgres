# mockgres

> In-memory Postgres-compatible engine for tests.

## Table of Contents
- [Overview](#overview)
- [Quickstart](#quickstart)
- [Running](#running)
- [Testing](#testing)
- [Features](#features)
- [Protocol Support Matrix](#protocol-support-matrix)
- [Multi-Statement Behavior Contract](#multi-statement-behavior-contract)
- [Troubleshooting Multi-Statement Clients](#troubleshooting-multi-statement-clients)
- [Limitations](#limitations)
- [Architecture Notes](#architecture-notes)
- [Roadmap](#roadmap)
- [Contributing](#contributing)
- [License](#license)

## Overview
Mockgres was born out of my frustration for the Postgres docker container taking too long, in my opinion, to start up.
I didn't want to have to write mocks, but I also wanted my unit tests to run as fast as possible.
I also didn't want to have to manage local installations and cleaning up in between test runs.

Mockgres aims to replicate a reasonable subset of Postgres functionality and semantics for two use cases.
The first use case is that of a typical CRUD app. The second use case is for a basic task queue using `SELECT FOR UPDATE SKIP LOCKED`.

## Quickstart
- Prereqs: Rust toolchain, `cargo`.
- `cargo run -p mockgres --bin mockgres -- --host 127.0.0.1 --port 6543`
- `cargo run -p mockgres --bin mockgres -- 127.0.0.1:6543`
- `psql -h 127.0.0.1 -p 6543 postgres`

## Running
Set the bind address via CLI options or `MOCKGRES_ADDR`.
The CLI accepts `--host`, `--port`, or a positional `host:port` string.
You can technically run it by embedding it as a library, all you really need to do is what's specified in `main`.

## Testing
- Commands:
  - `cargo fmt --all`
  - `cargo clippy --workspace --all-targets -- -D warnings`
  - `cargo test --workspace`
Testing covered pretty much exclusively by integration tests in the `tests` directory.

## Features
- Supported SQL surface (SELECT/INSERT/UPDATE/DELETE, joins, ON CONFLICT, etc.).
- PG wire protocol compatibility expectations.
- Locking semantics (FOR UPDATE, SKIP LOCKED).
- Copy-on-write snapshots via `mockgres_freeze()` and per-session `mockgres_reset()`.
- Type support basics.

## What's supported
- Core SQL: SELECT/INSERT/UPDATE/DELETE, WHERE/ORDER BY/LIMIT/OFFSET, projections/aliases, aggregates (count/sum/avg/min/max), GROUP BY/HAVING, simple scalar functions (now/current_timestamp/current_date/upper/lower/length/coalesce/abs/log/ln/greatest/extract epoch), type casts, interval literals, expressions
- Joins: CROSS/INNER/LEFT with ON predicates, multi-join, subqueries IN (SELECT ...)
- DML: INSERT ... ON CONFLICT DO NOTHING/DO UPDATE, UPDATE ... FROM, RETURNING
- Locking and tx: BEGIN/COMMIT/ROLLBACK (read committed only), SELECT FOR UPDATE SKIP LOCKED
- Copy-on-write snapshots: global freeze + per-session sandboxes (`mockgres_freeze()`, `mockgres_reset()`)
- types: int4/int8, float8, text/varchar, bool, date, timestamp/tz, bytea, interval, JSONB (no json ops though)
- Constraints/indices: primary key, unique, foreign key (cascade), create/drop index supported but no-op
- Catalog: schemas, databases (create and drop not supported), table create/drop, ALTER TABLE, `pg_catalog.pg_namespace`, `pg_catalog.pg_type` seeded for builtin types
- Wire protocol: simple and extended protocol

## Protocol Support Matrix

| Protocol path | Single statement | Multi-statement SQL (`;`) | Notes |
|---|---|---|---|
| Simple query (`Query` message / `simple_query`) | Supported | Supported | Executes in statement order and emits per-statement messages/tags. |
| Extended parse/bind/execute (`Parse` + `Bind` + `Execute`) | Supported | Not supported | Like PostgreSQL, prepared/parsed statements must contain one command. |

Known differences from PostgreSQL:
- Multi-statement execution support is provided only through the simple query protocol, not extended parse/bind/execute.

## Multi-Statement Behavior Contract
- Ordering: statements are planned and executed strictly left-to-right in one SQL string.
- Error short-circuit: execution stops at the first failing statement; later statements are not run.
- Simple protocol transaction semantics:
  - When already inside an explicit transaction (`BEGIN`), normal explicit transaction behavior applies.
  - For multi-statement simple-query messages outside an explicit transaction, Mockgres uses one implicit transaction for the message.
  - On any error in that implicit transaction, all earlier statements from that message are rolled back.
- Extended protocol parse contract:
  - Multiple non-empty statements in one prepared/parsed SQL string are rejected with a PostgreSQL-style parse error.
- Result framing:
  - Query statements emit row descriptions/data rows per statement.
  - Non-query statements emit command-complete tags per statement.
  - Empty statements (`;;` or trailing `;`) are treated as empty-query segments and do not emit command tags.

## Troubleshooting Multi-Statement Clients
- `batch_execute`/`simple_query` style APIs:
  - If a batch fails, verify whether your client sent simple or extended protocol before assuming rollback behavior.
  - In simple protocol multi-statement mode, a failure rolls back the whole message when not in an explicit transaction.
- Prepared statements and parse/execute flows:
  - Extended protocol parse/prepare accepts a single non-empty statement per SQL string.
  - Multi-statement SQL in prepared statements should return `cannot insert multiple commands into a prepared statement`.
- Protocol differences to debug quickly:
  - If command tags look incomplete, check for an early error that short-circuited the batch.
  - If row shape changes across statements, consume results as a stream of per-statement responses.

## Copy-on-write snapshots
- Freeze the current database state with `SELECT mockgres_freeze();`. The first call captures a base snapshot; subsequent calls are no-ops and return `true`.
- After freezing, every new session gets its own copy-on-write sandbox cloned from the frozen base. Changes made in one session stay isolated from others.
- Reset a pooled/reused connection with `SELECT mockgres_reset();` to discard that session’s sandbox and reclone from the frozen base on next use.
- Works in both simple and extended protocols, so it is safe to use with connection pools (run `mockgres_reset()` at the start of each test when reusing a pooled client).

Example (psql):
```
-- Seed baseline and freeze
create table items(id int primary key, label text);
insert into items values (1, 'a');
select mockgres_freeze(); -- returns t

-- Session A (shared DB) mutates baseline
insert into items values (2, 'b');

-- Session B (new connection) gets isolated sandbox
insert into items values (3, 'c');
select id from items order by id; -- sees 1,3 (not Session A’s 2)

-- Reset Session B sandbox (e.g., between tests)
select mockgres_reset(); -- returns t
select id from items order by id; -- back to frozen base: 1
```

## Architecture Notes
tbd

## Roadmap
Aims to be compatible with at least 1-2 most recent versions of Postgres.

## Contributing
tbd

## License
MIT
