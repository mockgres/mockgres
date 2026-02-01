#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${script_dir}/_common.sh"

port="$(random_port)"
tmp_dir="$(mktemp -d /tmp/mockgres-asyncpg-alembic-XXXXXX)"
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

python3 -m venv "${tmp_dir}/.venv"
"${tmp_dir}/.venv/bin/pip" install -q asyncpg==0.30.0

cat >"${tmp_dir}/test_asyncpg_alembic.py" <<'PY'
import asyncio
import asyncpg
import os

async def main():
    port = int(os.environ["MOCKGRES_PORT"])
    conn = await asyncpg.connect(
        user="postgres",
        database="postgres",
        host="127.0.0.1",
        port=port,
        ssl=False,
    )
    try:
        await conn.execute("create table alembic_demo(id int)")
        count = await conn.fetchval(
            "select count(*) from information_schema.tables where table_name = 'alembic_demo'"
        )
        print("table_count", count)
    finally:
        await conn.close()

asyncio.run(main())
PY

MOCKGRES_PORT="${port}" "${tmp_dir}/.venv/bin/python" "${tmp_dir}/test_asyncpg_alembic.py"
