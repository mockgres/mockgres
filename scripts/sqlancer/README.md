# SQLancer runner

This harness runs a pinned SQLancer source revision against a fresh Mockgres
process. It applies a small compatibility profile to SQLancer rather than
pretending Mockgres implements PostgreSQL's complete system catalog.

The profile enables three complementary SQLancer oracles while restricting
generated schemas and expressions to Mockgres's supported subset:

- TLP-WHERE compares a query with its true, false, and null predicate partitions
- TLP-HAVING applies the same partitioning to grouped and aggregate queries
- NoREC compares an optimized query with a count produced by an unoptimized form
- pivoted query synthesis (PQS) checks that a generated query returns its pivot row

- two to five columns per table using `BIGINT`, `BOOLEAN`, `TEXT`, and `DOUBLE
  PRECISION` (`PQS` retains the three exactly evaluable types)
- deterministic single-row inserts
- comparisons, boolean operations, null predicates, arithmetic, nested scalar
  functions, Cartesian products, and scope-correct `INNER`/`LEFT` joins
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

The default matrix runs 1,000 checks for each combination of `WHERE`, `NOREC`,
`PQS`, and `HAVING` with seeds `1`, `17`, `42`, `73`, and `101`: 20,000
deterministic checks in all. Expressions have depth four, tables receive up to
16 inserts, and strings grow to 32 characters. Every case gets a fresh Mockgres
process. Use environment variables to select or reproduce a subset:

```bash
SQLANCER_QUERIES=10000 SQLANCER_ORACLE=NOREC SQLANCER_SEED=42 scripts/sqlancer/run
SQLANCER_ORACLES=WHERE,PQS SQLANCER_SEEDS=7,11 scripts/sqlancer/run
```

For continuous or pre-release exploration, the soak runner covers 50 new
consecutive seeds and 500,000 checks by default:

```bash
scripts/sqlancer/soak
SQLANCER_SEED_START=2000 SQLANCER_SEED_COUNT=100 scripts/sqlancer/soak
```

The soak range is deterministic, so every failure remains reproducible. Change
`SQLANCER_SEED_START` between runs to explore fresh space without weakening the
repeatable default gate.

Set `MOCKGRES_JAVA=/path/to/java` when Java is not available through
`JAVA_HOME`, `PATH`, or Maven's runtime. Run `scripts/sqlancer/run --help` for
all settings.

The command exits nonzero on a SQLancer finding, setup failure, timeout, or
server crash. Artifacts are written under `target/sqlancer/runs`; each case has
the Mockgres server log, SQLancer output, and any reproducer logs, while
`summary.tsv` records the matrix results.

## Updating SQLancer

Update `sqlancer_revision` in `run`, recreate `sqlancer.patch` against that
exact revision, and run the default campaign. The patch is intentionally kept
separate from Mockgres so the upstream test generator remains auditable.
