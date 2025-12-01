# mockgres

> In-memory Postgres-compatible engine for tests.

## Table of Contents
- [Overview](#overview)
- [Quickstart](#quickstart)
- [Running](#running)
- [Testing](#testing)
- [Features](#features)
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
- `cargo run -p mockgres --bin mockgres -- 127.0.0.1:6543`
- `psql -h 127.0.0.1 -p 6543 postgres`

## Running
Currently, all you really need to do is specify `MOCKGRES_ADDR`.
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
- Type support basics.

## What's supported
- Core SQL: SELECT/INSERT/UPDATE/DELETE, WHERE/ORDER BY/LIMIT/OFFSET, projections/aliases, aggregates (count/sum/avg/min/max), GROUP BY/HAVING, simple scalar functions (now/current_timestamp/current_date/upper/lower/length/coalesce/abs/log/ln/greatest/extract epoch), type casts, interval literals, expressions
- Joins: CROSS/INNER/LEFT with ON predicates, multi-join, subqueries IN (SELECT ...)
- DML: INSERT ... ON CONFLICT DO NOTHING/DO UPDATE, UPDATE ... FROM, RETURNING
- Locking and tx: BEGIN/COMMIT/ROLLBACK (read committed only), SELECT FOR UPDATE SKIP LOCKED
- types: int4/int8, float8, text/varchar, bool, date, timestamp/tz, bytea, interval, JSONB (no json ops though)
- Constraints/indices: primary key, unique, foreign key (cascade), create/drop index supported but no-op
- Catalog: schemas, databases (create and drop not supported), table create/drop, ALTER TABLE
- Wire protocol: simple and extended protocol

## Architecture Notes
tbd

## Roadmap
Aims to be compatible with at least 1-2 most recent versions of Postgres.

## Contributing
tbd

## License
MIT