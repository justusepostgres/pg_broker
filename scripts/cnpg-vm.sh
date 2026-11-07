#!/usr/bin/env bash
# Run on the dedicated Ubuntu test VM, never on a shared Kubernetes host.
set -euo pipefail
cd "$(dirname "$0")/.."
k() { sudo k3s kubectl "$@"; }
case "${1:-help}" in
 bootstrap)
  if ! command -v k3s >/dev/null; then
    curl -fsSL https://get.k3s.io -o /tmp/pg-broker-install-k3s.sh
    sudo env INSTALL_K3S_VERSION=v1.35.1+k3s1 sh /tmp/pg-broker-install-k3s.sh --disable traefik --disable servicelb
  fi
  k apply --server-side -f https://raw.githubusercontent.com/cloudnative-pg/cloudnative-pg/release-1.30/releases/cnpg-1.30.1.yaml
  k -n cnpg-system rollout status deployment/cnpg-controller-manager --timeout=180s
  ;;
 build)
  docker build --target cnpg -t pg-broker-postgres:17.5-0.1.7 .
  docker build --target gateway -t pg-broker-gateway:0.1.7 .
  ;;
 deploy)
  docker save pg-broker-postgres:17.5-0.1.7 pg-broker-gateway:0.1.7 | sudo k3s ctr images import -
  k apply -k deploy/cnpg
  k -n pg-broker wait cluster/broker-db --for=condition=Ready --timeout=600s
  k -n pg-broker rollout status deployment/gateway --timeout=180s
  ;;
 client)
  k apply -f deploy/cnpg/client.yaml
  k -n pg-broker wait pod/kafka-client --for=condition=Ready --timeout=180s
  k -n pg-broker cp tests/cnpg_smoke.py kafka-client:/tmp/cnpg_smoke.py
  ;;
 seed|resume|no-standby) k -n pg-broker exec kafka-client -- python /tmp/cnpg_smoke.py "$1" ;;
 restart-gateways)
  k -n pg-broker rollout restart deployment/gateway
  k -n pg-broker rollout status deployment/gateway --timeout=180s
  ;;
 status) k -n pg-broker get clusters,pods,services ;;
 *) echo 'Usage: scripts/cnpg-vm.sh {bootstrap|build|deploy|client|seed|resume|no-standby|restart-gateways|status}' ;;
esac
