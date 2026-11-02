# CloudNativePG deployment

This directory targets **CNPG 1.30.1**, PostgreSQL **17.5**, and Kubernetes **1.33.1**.
It packages the extension inside the official CNPG PostgreSQL image (pinned by
manifest digest), installs it during application-database bootstrap, and connects
two separate gateway Pods to CNPG's `broker-db-rw` Service.

**Status: manifests render, but live deployment is not yet verified.** A local
kind/CNPG verification attempt exhausted host disk during image downloads; the
failed disposable VM was removed. Do not interpret these manifests as a successful
cluster test. Allow at least 15 GiB of free host storage for images, build layers,
Kubernetes and database volumes.

## Local verification

With Docker, kind and kubectl installed, select a local Docker context explicitly:

```sh
export DOCKER_CONTEXT=your-local-context
scripts/cnpg-local.sh build
scripts/cnpg-local.sh cluster
scripts/cnpg-local.sh deploy
scripts/cnpg-local.sh status
```

The script always uses `.dev/cnpg-kubeconfig`, never the active remote context.
It creates only the `pg-broker-cnpg` kind cluster. Delete it using
`scripts/cnpg-local.sh delete` when finished.

## Existing clusters

Build and push the `cnpg` and `gateway` Docker targets to your own registry, then
change `spec.imageName` in `cluster.yaml` and the Deployment's gateway image before
applying the Kustomization with an explicitly selected Kubernetes context.
The PostgreSQL image tag must start with `17` (for example
`registry.example/pg-broker:17.5-0.1.1`), as CNPG uses that to recognize the major version.
Configure imagePullSecrets and storageClass as required by your cluster.
No registry publishing or remote-cluster changes are performed by the local script.

CNPG bootstraps `broker` with application owner `broker_app`, creates the extension
as postgres, and grants the application role only execution of the public broker
function. The gateway reads the generated `broker-db-app` Secret using separate
username/password fields, and mounts the `broker-db-ca` certificate. `PGSSLROOTCERT`
forces TLS with CA and hostname verification. Plaintext PostgreSQL connections
are rejected by this Cluster's HBA rule. No preload setting is needed.

Three PostgreSQL instances use required synchronous replication to one standby.
An acknowledged commit therefore waits for PostgreSQL WAL synchronization, rather
than merely relying on the gateway's Kafka acknowledgment setting. If no synchronous
standby is available, writes stop. Do not change `dataDurability` to `preferred`
while continuing to promise acknowledged-record survival across primary failure.

## Kafka connectivity

In-cluster Kafka clients use `broker.pg-broker.svc.cluster.local:9092`. Both gateways
advertise this same Service address and logical broker ID, so metadata discovery
and coordinator discovery remain routable. Each connection can reach either
replica because group generations and assignments live in PostgreSQL.

A plain `kubectl port-forward` is insufficient for external Kafka clients if
metadata still advertises the cluster-only DNS address. For external access,
provide a TCP LoadBalancer or equivalent and set **the same reachable hostname
and port** in `ADVERTISED_HOST`/`ADVERTISED_PORT` on every gateway replica. Kafka
transport is currently plaintext and unauthenticated; this deployment is intended
for a trusted network. Database TLS does not add Kafka-side TLS or ACL support.

## Required live acceptance checks

Before claiming this deployment verified:

1. Build both targets and run server-side manifest validation against CNPG's CRDs.
2. Wait for three healthy database Pods and two ready gateways.
3. Verify `pg_extension.extversion='0.1.1'` in `broker`, and verify gateway
   sessions are TLS sessions using `pg_stat_ssl`.
4. Run ordinary kafka-python 2.0.2 and confluent-kafka 2.6.1 clients inside the
   cluster: create, produce, subscribe, commit and fetch committed offsets.
5. Restart the gateway Deployment and repeat consumption with the same group.
6. Promote a standby using CNPG's supported switchover, wait for readiness,
   reconnect through the unchanged Services, and verify records and offsets.
7. Confirm no successful Produce acknowledgment while required synchronous
   replication has no eligible standby.

Upgrade the extension image on **all database instances** before running
`ALTER EXTENSION pg_broker UPDATE`. The bundled 0.1.0→0.1.1 SQL upgrade preserves
records and group state. Bootstrap SQL runs only at initial creation; it is not
an ongoing upgrade controller. Configure backups/WAL archiving and test restores
separately before production use.

Primary references: [CNPG image requirements](https://cloudnative-pg.io/docs/1.30/container_images/),
[bootstrap](https://cloudnative-pg.io/docs/1.30/bootstrap/),
[synchronous replication](https://cloudnative-pg.io/docs/1.30/replication/).
