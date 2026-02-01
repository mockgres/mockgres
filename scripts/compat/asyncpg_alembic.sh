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
"${tmp_dir}/.venv/bin/pip" install -q asyncpg==0.30.0 alembic

mkdir -p "${tmp_dir}/alembic/versions"

cat >"${tmp_dir}/alembic.ini" <<'INI'
[alembic]
script_location = alembic
prepend_sys_path = .
sqlalchemy.url = postgresql+asyncpg://postgres@127.0.0.1/postgres

[loggers]
keys = root,sqlalchemy,alembic

[handlers]
keys = console

[formatters]
keys = generic

[logger_root]
level = WARNING
handlers = console

[logger_sqlalchemy]
level = WARNING
handlers =
qualname = sqlalchemy.engine

[logger_alembic]
level = INFO
handlers =
qualname = alembic

[handler_console]
class = StreamHandler
args = (sys.stderr,)
level = NOTSET
formatter = generic

[formatter_generic]
format = %(levelname)-5.5s [%(name)s] %(message)s
INI

cat >"${tmp_dir}/alembic/env.py" <<'PY'
import asyncio
import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import pool
from sqlalchemy.ext.asyncio import create_async_engine

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = None
db_url = os.environ["DATABASE_URL"]

def do_run_migrations(connection):
    context.configure(connection=connection, target_metadata=target_metadata)
    with context.begin_transaction():
        context.run_migrations()

async def run_migrations_online() -> None:
    connectable = create_async_engine(db_url, poolclass=pool.NullPool)
    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)
    await connectable.dispose()

if context.is_offline_mode():
    raise RuntimeError("offline mode not supported")
else:
    asyncio.run(run_migrations_online())
PY

cat >"${tmp_dir}/alembic/versions/001_create_table.py" <<'PY'
from alembic import op
import sqlalchemy as sa

revision = "001"
down_revision = None
branch_labels = None
depends_on = None

def upgrade():
    op.create_table(
        "alembic_demo",
        sa.Column("id", sa.Integer()),
    )

def downgrade():
    op.drop_table("alembic_demo")
PY

cat >"${tmp_dir}/alembic/versions/002_add_column.py" <<'PY'
from alembic import op
import sqlalchemy as sa

revision = "002"
down_revision = "001"
branch_labels = None
depends_on = None

def upgrade():
    op.add_column("alembic_demo", sa.Column("name", sa.Text()))

def downgrade():
    op.drop_column("alembic_demo", "name")
PY

(
  cd "${tmp_dir}"
  DATABASE_URL="postgresql+asyncpg://postgres@127.0.0.1:${port}/postgres" \
    "${tmp_dir}/.venv/bin/alembic" -c "${tmp_dir}/alembic.ini" upgrade head
)
