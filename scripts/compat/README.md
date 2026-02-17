# Compatibility Scripts

These scripts run compatibility checks outside the `cargo test` harness.
They each start a mockgres instance on a random local port and run the
corresponding client or migration tool against it.

Notes:
- These scripts run `cargo run -p mockgres --bin mockgres` from the repo root.
- These scripts create temporary directories under `/tmp`.
- Python/Go dependencies are downloaded on first run.

Run examples:

```bash
scripts/compat/asyncpg_basic.sh
scripts/compat/asyncpg_alembic.sh
scripts/compat/go_pgx.sh
scripts/compat/go_pgx_multi_protocol.sh
scripts/compat/go_pq.sh
scripts/compat/golang_migrate.sh
```
