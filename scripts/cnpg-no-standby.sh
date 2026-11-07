#!/usr/bin/env bash
# Fault injection for the disposable, three-instance K3s deployment only.
set -euo pipefail
cd "$(dirname "$0")/.."
k() { sudo k3s kubectl -n pg-broker "$@"; }
[[ $(k get cluster broker-db -o jsonpath='{.spec.instances}') == 3 ]]
k wait cluster/broker-db --for=condition=Ready --timeout=180s
primary=$(k get cluster broker-db -o jsonpath='{.status.currentPrimary}')
[[ $primary =~ ^broker-db-[123]$ ]]
existing=$(k get cluster broker-db -o jsonpath='{.metadata.annotations.cnpg\.io/fencedInstances}')
[[ -z $existing || $existing == '[]' ]]
standbys=()
for number in 1 2 3; do
  [[ broker-db-$number == "$primary" ]] || standbys+=("broker-db-$number")
done
restore() { k annotate cluster broker-db 'cnpg.io/fencedInstances=[]' --overwrite; }
trap restore EXIT
fenced=$(printf '["%s","%s"]' "${standbys[0]}" "${standbys[1]}")
k annotate cluster broker-db "cnpg.io/fencedInstances=$fenced" --overwrite
sql() { k exec "$primary" -c postgres -- psql -U postgres -d broker -Atc "$1"; }
for attempt in {1..60}; do
  [[ $(sql 'SELECT count(*) FROM pg_stat_replication') == 0 ]] && break
  sleep 1
done
[[ $(sql 'SELECT count(*) FROM pg_stat_replication') == 0 ]]
[[ -n $(sql 'SHOW synchronous_standby_names') ]]
sql 'SHOW synchronous_standby_names'
scripts/cnpg-vm.sh no-standby
restore
trap - EXIT
k wait cluster/broker-db --for=condition=Ready --timeout=180s
# The timed-out write may exist. Use a fresh topic to prove service recovery.
scripts/cnpg-vm.sh seed
scripts/cnpg-vm.sh resume
