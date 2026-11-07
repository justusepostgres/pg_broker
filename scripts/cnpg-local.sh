#!/usr/bin/env bash
# Explicit context and isolated kubeconfig: never acts on the user's current cluster.
set -euo pipefail
cd "$(dirname "$0")/.."
: "${DOCKER_CONTEXT:?Set DOCKER_CONTEXT to an available local Docker engine}"
mkdir -p .dev
export KUBECONFIG="$PWD/.dev/cnpg-kubeconfig"
cluster=pg-broker-cnpg
case "${1:-help}" in
 build)
  docker build --target cnpg -t pg-broker-postgres:17.5-0.1.7 .
  docker build --target gateway -t pg-broker-gateway:0.1.7 .
  ;;
 cluster)
  kind create cluster --name "$cluster" --image kindest/node:v1.33.1 --kubeconfig "$KUBECONFIG" --wait 60s
  kubectl apply --server-side -f https://raw.githubusercontent.com/cloudnative-pg/cloudnative-pg/release-1.30/releases/cnpg-1.30.1.yaml
  kubectl -n cnpg-system rollout status deployment/cnpg-controller-manager --timeout=180s
  ;;
 deploy)
  kind load docker-image --name "$cluster" pg-broker-postgres:17.5-0.1.7 pg-broker-gateway:0.1.7
  kubectl apply -k deploy/cnpg
  kubectl -n pg-broker wait cluster/broker-db --for=condition=Ready --timeout=600s
  kubectl -n pg-broker rollout status deployment/gateway --timeout=180s
  ;;
 status)
  kubectl -n pg-broker get clusters,pods,services
  ;;
 delete)
  kind delete cluster --name "$cluster"
  ;;
 *) echo 'Usage: scripts/cnpg-local.sh {build|cluster|deploy|status|delete}' ;;
esac
