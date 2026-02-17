#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${script_dir}/_common.sh"

if ! command -v go >/dev/null 2>&1; then
  echo "go not found in PATH" >&2
  exit 1
fi

port="$(random_port)"
tmp_dir="$(mktemp -d /tmp/mockgres-go-pgx-multi-protocol-XXXXXX)"
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
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func rowCount(ctx context.Context, pool *pgxpool.Pool, table string) int {
	var count int
	query := fmt.Sprintf("select count(*) from %s", table)
	if err := pool.QueryRow(ctx, query).Scan(&count); err != nil {
		log.Fatalf("count rows: %v", err)
	}
	return count
}

func main() {
	ctx := context.Background()
	url := os.Getenv("PG_URL")
	const table = "compat_multi_mode"

	simpleCfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		log.Fatalf("parse simple config: %v", err)
	}
	simpleCfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	simplePool, err := pgxpool.NewWithConfig(ctx, simpleCfg)
	if err != nil {
		log.Fatalf("connect simple pool: %v", err)
	}
	defer simplePool.Close()

	extendedCfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		log.Fatalf("parse extended config: %v", err)
	}
	extendedCfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec
	extendedPool, err := pgxpool.NewWithConfig(ctx, extendedCfg)
	if err != nil {
		log.Fatalf("connect extended pool: %v", err)
	}
	defer extendedPool.Close()

	if _, err := simplePool.Exec(ctx, "drop table if exists "+table); err != nil {
		log.Fatalf("drop table: %v", err)
	}
	if _, err := simplePool.Exec(ctx, "create table "+table+"(id int primary key)"); err != nil {
		log.Fatalf("create table: %v", err)
	}
	if _, err := simplePool.Exec(ctx, "insert into "+table+" values (1); insert into "+table+" values (2)"); err != nil {
		log.Fatalf("simple multi-statement insert: %v", err)
	}
	simpleCount := rowCount(ctx, simplePool, table)
	if simpleCount != 2 {
		log.Fatalf("expected 2 rows after simple multi insert, got %d", simpleCount)
	}
	if _, err := simplePool.Exec(ctx, "insert into "+table+" values (3); insert into "+table+" values ('bad')"); err == nil {
		log.Fatalf("expected simple multi-statement error")
	}
	simpleAfterError := rowCount(ctx, simplePool, table)
	if simpleAfterError != 2 {
		log.Fatalf("simple protocol rollback failed, expected 2 rows got %d", simpleAfterError)
	}
	fmt.Printf("simple_multi_ok=%d\n", simpleAfterError)

	conn, err := extendedPool.Acquire(ctx)
	if err != nil {
		log.Fatalf("acquire extended conn: %v", err)
	}
	defer conn.Release()
	_, err = conn.Conn().Prepare(ctx, "stmt_multi", "insert into "+table+" values (3); insert into "+table+" values (4)")
	if err == nil {
		log.Fatalf("expected extended multi-statement parse error")
	}
	if !strings.Contains(err.Error(), "cannot insert multiple commands into a prepared statement") {
		log.Fatalf("unexpected extended multi-statement error: %v", err)
	}
	extendedCount := rowCount(ctx, extendedPool, table)
	if extendedCount != 2 {
		log.Fatalf("expected 2 rows after rejected extended multi insert, got %d", extendedCount)
	}
	fmt.Printf("extended_multi_rejected_ok=%d\n", extendedCount)
}
GO

cd "${tmp_dir}"
go mod init example.com/mockgres-pgx-multi-protocol >/dev/null
go mod tidy >/dev/null
PG_URL="postgres://postgres@127.0.0.1:${port}/postgres?sslmode=disable" go run .
