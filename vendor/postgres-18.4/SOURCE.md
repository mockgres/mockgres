# PostgreSQL 18.4 regression test assets

These files were extracted from the official PostgreSQL 18.4 source archive:

- Source: https://ftp.postgresql.org/pub/source/v18.4/postgresql-18.4.tar.gz
- SHA-256: `450aa8f2da06c46f8221916e82ae06b04fb1040f8f00643dbf8b7d663caac0b9`
- Extracted paths: `src/test/regress/sql`, `expected`, `data`,
  `parallel_schedule`, and `resultmap`

The files are intentionally pinned to the PostgreSQL compatibility version
reported by Mockgres. Update the archive, checksum, assets, and Mockgres version
constants together.

The PostgreSQL license and copyright notice are preserved in `COPYRIGHT`.
