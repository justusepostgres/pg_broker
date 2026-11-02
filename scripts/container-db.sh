#!/usr/bin/env bash
set -euo pipefail
if [[ ! -f /data/PG_VERSION ]]; then
  initdb -D /data -U postgres --auth-local=trust --auth-host=scram-sha-256 --pwfile=<(printf '%s' "${POSTGRES_PASSWORD:?}")
  printf '\nhost all all 0.0.0.0/0 scram-sha-256\n' >> /data/pg_hba.conf
  pg_ctl -D /data -o '-h 127.0.0.1' -w start
  psql -U postgres -v ON_ERROR_STOP=1 -c 'CREATE EXTENSION pg_broker'
  pg_ctl -D /data -m fast -w stop
fi
touch /data/.broker-ready
exec postgres -D /data -h 0.0.0.0
