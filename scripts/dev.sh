#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
export PG_CONFIG=${PG_CONFIG:-pg_config}
export PGRX_PG_CONFIG_PATH="$PG_CONFIG"
pgbin=$($PG_CONFIG --bindir)
export DATABASE_URL=${DATABASE_URL:-'host=127.0.0.1 port=55432 user=postgres dbname=postgres'}
mkdir -p .dev
case "${1:-help}" in
 build)
  cargo build --workspace --locked
  scripts/package-extension.sh
  ;;
 init)
  [[ -f .dev/pgdata/PG_VERSION ]] || "$pgbin/initdb" -D .dev/pgdata -U postgres --auth-local=trust --auth-host=trust --no-locale -E UTF8
  "$pgbin/pg_ctl" -D .dev/pgdata status >/dev/null 2>&1 || "$pgbin/pg_ctl" -D .dev/pgdata -l .dev/postgres.log -o '-p 55432 -h 127.0.0.1 -k /tmp' start
  "$pgbin/psql" "$DATABASE_URL" -v ON_ERROR_STOP=1 -c 'CREATE EXTENSION IF NOT EXISTS pg_broker'
  ;;
 gateway) exec target/debug/pg-broker-gateway ;;
 test) cargo test --locked; .venv/bin/pytest -q tests ;;
 stop) "$pgbin/pg_ctl" -D .dev/pgdata -m fast stop ;;
 *) echo 'Usage: scripts/dev.sh {build|init|gateway|test|stop}' ;;
esac
