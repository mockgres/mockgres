#!/usr/bin/env bash
set -euo pipefail

common_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${common_dir}/../.." && pwd)"

require_cargo() {
  if ! command -v cargo >/dev/null 2>&1; then
    echo "cargo not found in PATH" >&2
    return 1
  fi
}

start_mockgres() {
  local port="$1"
  local log_file="$2"

  require_cargo
  (cd "${repo_root}" && cargo run -q -p mockgres --bin mockgres -- "127.0.0.1:${port}") \
    >"${log_file}" 2>&1 &
  MOCKGRES_PID=$!
}

random_port() {
  python3 - <<'PY'
import socket
s = socket.socket()
s.bind(('127.0.0.1', 0))
print(s.getsockname()[1])
s.close()
PY
}

wait_for_port() {
  local host="$1"
  local port="$2"
  python3 - <<PY
import socket, time, sys
host="$host"
port=int("$port")
for _ in range(200):
    try:
        with socket.create_connection((host, port), timeout=0.2):
            sys.exit(0)
    except OSError:
        time.sleep(0.05)
print("server did not start")
sys.exit(1)
PY
}
