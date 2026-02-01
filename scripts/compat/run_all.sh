#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

"${script_dir}/asyncpg_basic.sh"
"${script_dir}/asyncpg_alembic.sh"
"${script_dir}/go_pgx.sh"
"${script_dir}/go_pq.sh"
"${script_dir}/golang_migrate.sh"
