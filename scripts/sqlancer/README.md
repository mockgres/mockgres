# SQLancer runner

This harness runs a pinned SQLancer source revision against a fresh Mockgres
process. It applies a small compatibility profile to SQLancer rather than
pretending Mockgres implements PostgreSQL's complete system catalog.

The profile keeps SQLancer's TLP-WHERE oracle and result comparison, while
restricting generated schemas and expressions to Mockgres's supported subset:

- `BIGINT`, `BOOLEAN`, and `TEXT` columns
- deterministic single-row inserts
- comparisons, boolean operations, null predicates, arithmetic, supported
  scalar functions, and supported joins
- JDBC result metadata for schema discovery instead of PostgreSQL-only catalog
  queries

SQLancer is pinned to commit
`af6ae85d0679c5a153d8e1cbf22c4e89980c0f68`. The runner clones and builds it
under `target/sqlancer/cache`, so no SQLancer source or build output is
committed.

## Requirements

- Rust and Cargo
- Git
- Java 11 or newer
- Maven
- Python 3

## Usage

```bash
scripts/sqlancer/run
```

The default smoke campaign runs 100 deterministic oracle checks with seed `1`.
Use environment variables to expand or reproduce it:

```bash
SQLANCER_QUERIES=10000 SQLANCER_SEED=42 scripts/sqlancer/run
```

Set `MOCKGRES_JAVA=/path/to/java` when Java is not available through
`JAVA_HOME`, `PATH`, or Maven's runtime. Run `scripts/sqlancer/run --help` for
all settings.

The command exits nonzero on a SQLancer finding, setup failure, timeout, or
server crash. Artifacts are written under `target/sqlancer/runs` and include
the Mockgres server log, SQLancer output, and any SQLancer reproducer logs.

## Updating SQLancer

Update `sqlancer_revision` in `run`, recreate `sqlancer.patch` against that
exact revision, and run the default campaign. The patch is intentionally kept
separate from Mockgres so the upstream test generator remains auditable.
