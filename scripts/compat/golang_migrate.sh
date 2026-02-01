#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${script_dir}/_common.sh"

if ! command -v go >/dev/null 2>&1; then
  echo "go not found in PATH" >&2
  exit 1
fi

port="$(random_port)"
tmp_dir="$(mktemp -d /tmp/mockgres-golang-migrate-XXXXXX)"
log_file="${tmp_dir}/mockgres.log"

cleanup() {
  if [ -n "${server_pid:-}" ] && kill -0 "${server_pid}" 2>/dev/null; then
    kill "${server_pid}"
    wait "${server_pid}" || true
  fi
}
trap cleanup EXIT

start_mockgres "${port}" "${log_file}"
server_pid="${MOCKGRES_PID}"

wait_for_port "127.0.0.1" "${port}"

mkdir -p "${tmp_dir}/migrations"
cat >"${tmp_dir}/migrations/1_create_table.up.sql" <<'SQL'
CREATE TABLE foo(id INT);
SQL
cat >"${tmp_dir}/migrations/1_create_table.down.sql" <<'SQL'
DROP TABLE foo;
SQL

cat >"${tmp_dir}/main.go" <<'GO'
package main

import (
  "database/sql"
  "fmt"
  "log"
  "os"

  "github.com/golang-migrate/migrate/v4"
  "github.com/golang-migrate/migrate/v4/database/postgres"
  _ "github.com/golang-migrate/migrate/v4/source/file"
  _ "github.com/lib/pq"
)

func main() {
  url := os.Getenv("PG_URL")
  migrations := os.Getenv("MIGRATIONS")

  db, err := sql.Open("postgres", url)
  if err != nil {
    log.Fatalf("open: %v", err)
  }
  defer db.Close()
  if err := db.Ping(); err != nil {
    log.Fatalf("ping: %v", err)
  }

  driver, err := postgres.WithInstance(db, &postgres.Config{})
  if err != nil {
    log.Fatalf("driver: %v", err)
  }

  m, err := migrate.NewWithDatabaseInstance("file://"+migrations, "postgres", driver)
  if err != nil {
    log.Fatalf("migrate: %v", err)
  }

  if err := m.Up(); err != nil && err != migrate.ErrNoChange {
    log.Fatalf("up: %v", err)
  }

  v, dirty, err := m.Version()
  if err != nil {
    log.Fatalf("version: %v", err)
  }
  fmt.Printf("version=%d dirty=%v\n", v, dirty)

  var count int
  if err := db.QueryRow("select count(*) from information_schema.tables where table_name = 'foo'").Scan(&count); err != nil {
    log.Fatalf("count: %v", err)
  }
  fmt.Printf("table_count=%d\n", count)
}
GO

cd "${tmp_dir}"
go mod init example.com/mockgres-golang-migrate >/dev/null
go mod tidy >/dev/null
PG_URL="postgres://postgres@127.0.0.1:${port}/postgres?sslmode=disable" \
MIGRATIONS="${tmp_dir}/migrations" \
go run .
