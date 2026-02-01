#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${script_dir}/_common.sh"

if ! command -v go >/dev/null 2>&1; then
  echo "go not found in PATH" >&2
  exit 1
fi

port="$(random_port)"
tmp_dir="$(mktemp -d /tmp/mockgres-go-pgx-XXXXXX)"
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

cat >"${tmp_dir}/main.go" <<'GO'
package main

import (
  "context"
  "fmt"
  "log"
  "os"

  "github.com/jackc/pgx/v5/pgxpool"
)

func main() {
  url := os.Getenv("PG_URL")
  pool, err := pgxpool.New(context.Background(), url)
  if err != nil {
    log.Fatalf("connect: %v", err)
  }
  defer pool.Close()

  var v int
  if err := pool.QueryRow(context.Background(), "select 1").Scan(&v); err != nil {
    log.Fatalf("query: %v", err)
  }
  fmt.Printf("select1=%d\n", v)
}
GO

cd "${tmp_dir}"
go mod init example.com/mockgres-pgx >/dev/null
go mod tidy >/dev/null
PG_URL="postgres://postgres@127.0.0.1:${port}/postgres?sslmode=disable" go run .
