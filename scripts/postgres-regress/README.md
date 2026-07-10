# PostgreSQL regression runner

This runner executes the unmodified PostgreSQL 18.4 core regression suite
against one fresh Mockgres process. It deliberately runs the upstream parallel
schedule serially while compatibility work is in progress.

## Requirements

- Rust and Cargo
- Python 3
- PostgreSQL 18 `psql`
- `timeout` on Linux or `gtimeout` on macOS when per-test timeouts are desired

Set `MOCKGRES_PSQL=/path/to/psql` when PostgreSQL 18 is not first on `PATH`.

## Usage

```bash
# List the pinned schedule
scripts/postgres-regress/run --list

# Run test_setup followed by selected tests
scripts/postgres-regress/run boolean int4

# Run the complete schedule serially
scripts/postgres-regress/run
```

The runner starts Mockgres with the bootstrap `postgres` database, creates an
isolated `regression` database using PostgreSQL's regression options, and then
runs `test_setup` plus the requested schedule. Database state is shared between
test files as PostgreSQL expects. Freeze/reset behavior is not involved.

Each test is classified as `PASS`, `FAIL`, `PSQL_FAIL`, `TIMEOUT`, or
`CRASHED`. Exact upstream output remains the oracle; known differences are not
rewritten or silently accepted. Artifacts include:

- `status.tsv` and `summary.json`
- raw output under `results/`
- the closest expected-output diff under `diffs/`
- `mockgres.log` and `create-database.log`

Artifacts default to `target/postgres-regress/18.4/<run-id>`. Set `OUTDIR` to
choose a stable location. The command exits nonzero unless every executed test
passes; set `MOCKGRES_REGRESS_ALLOW_FAILURES=1` only for baseline collection.

For harness development, `MOCKGRES_ALLOW_PSQL_VERSION_MISMATCH=1` permits an
older `psql`, but its output is not a valid PostgreSQL 18 compatibility result.

## Pinned source

The vendored files come from the official PostgreSQL 18.4 source archive and
retain its license. See `vendor/postgres-18.4/SOURCE.md` for the URL, SHA-256,
and exact extracted paths.
