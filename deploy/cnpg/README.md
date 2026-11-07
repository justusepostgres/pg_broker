# CloudNativePG deployment

This directory targets **CNPG 1.30.1**, PostgreSQL **17.5**, and **K3s v1.35.1+k3s1** (Kubernetes 1.35.1). The optional kind script pins Kubernetes 1.33.1.
It packages the extension inside the official CNPG PostgreSQL image (pinned by
manifest digest), installs it during application-database bootstrap, and connects
two separate gateway Pods to CNPG's `broker-db-rw` Service.

**Verified on 2026-11-02:** three database Pods, two gateway Pods, extension
0.1.1, encrypted database sessions, both Kafka clients, committed-offset resume
after gateway restart and CNPG switchover, and failed Produce acknowledgment
when both synchronous standbys were fenced. See [evidence](../../docs/verification.md).
The test used one dedicated OCI ARM VM: it verifies replication and process
recovery, **not availability after physical-host or availability-zone loss**.

## Dedicated VM verification (tested)

On a disposable Ubuntu ARM VM with Docker installed, a Docker-enabled user,
sudo access, 4 CPUs, 24 GiB RAM and at least 30 GiB free disk:

```sh
scripts/cnpg-vm.sh bootstrap
scripts/cnpg-vm.sh build
scripts/cnpg-vm.sh deploy
scripts/cnpg-vm.sh client
scripts/cnpg-vm.sh seed
scripts/cnpg-vm.sh restart-gateways
scripts/cnpg-vm.sh resume
```

These commands use `sudo k3s kubectl` on that machine. Bootstrap installs an
operator; run them only on a dedicated test VM. Keep SSH access restricted and
do not expose the Kubernetes API or Kafka port publicly. The scripts do not
publish images or act on your workstation's current Kubernetes context.

Install the matching CNPG 1.30.1 kubectl plugin, then select a current standby
(the test promoted `broker-db-2` from an initial `broker-db-1` primary):

```sh
sudo env KUBECONFIG=/etc/rancher/k3s/k3s.yaml kubectl-cnpg -n pg-broker promote broker-db 2
sudo k3s kubectl -n pg-broker wait cluster/broker-db --for=jsonpath='{.status.currentPrimary}'=broker-db-2 --timeout=180s
sudo k3s kubectl -n pg-broker wait cluster/broker-db --for=condition=Ready --timeout=180s
scripts/cnpg-vm.sh resume
scripts/cnpg-no-standby.sh
```

The last script fences both standbys, asserts replication connections are gone
and synchronous replication is still required, checks Produce fails without an
acknowledgment, restores fencing via an EXIT trap, and checks recovery with a
fresh topic. A failed Produce may still have committed locally, so it must not
be treated as a rolled-back write. Use this fault injection only on disposable
clusters. Destroy the dedicated VM and its disks when finished.

## Local verification

This alternative was not run to completion because the local host lacked disk space.
Allow at least 15 GiB free storage. With Docker, kind and kubectl installed, select
a local Docker context explicitly:

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
`registry.example/pg-broker:17.5-0.1.7`), as CNPG uses that to recognize the major version.
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

## Acceptance coverage

The dedicated VM run passed these checks:

1. Build both targets and run server-side manifest validation against CNPG's CRDs.
2. Wait for three healthy database Pods and two ready gateways.
3. Verify `pg_extension.extversion='0.1.7'` in `broker`, and verify gateway
   sessions are TLS sessions using `pg_stat_ssl`.
4. Run ordinary kafka-python 2.0.2 and confluent-kafka 2.6.1 clients inside the
   cluster: create, produce, subscribe, commit and fetch committed offsets.
5. Restart the gateway Deployment and repeat consumption with the same group.
6. Promote a standby using CNPG's supported switchover, wait for readiness,
   reconnect through the unchanged Services, and verify records and offsets.
7. Confirm no successful Produce acknowledgment while required synchronous
   replication has no eligible standby.

Upgrade the extension image on **all database instances** before running
`ALTER EXTENSION pg_broker UPDATE`. The bundled 0.1.0→0.1.1→0.1.2→0.1.3→0.1.4→0.1.5→0.1.6→0.1.7 SQL upgrade chain preserves
records and group state. Bootstrap SQL runs only at initial creation; it is not
an ongoing upgrade controller. Configure backups/WAL archiving and test restores
separately before production use.

Primary references: [CNPG image requirements](https://cloudnative-pg.io/docs/1.30/container_images/),
[bootstrap](https://cloudnative-pg.io/docs/1.30/bootstrap/),
[synchronous replication](https://cloudnative-pg.io/docs/1.30/replication/),
and [CNPG fencing](https://github.com/cloudnative-pg/cloudnative-pg/blob/main/docs/src/fencing.md).

Current manifests build extension 0.1.7, which skips batch-index searches for caught-up fetches.
Version 0.1.6 avoids scanning live retention prefixes.
Version 0.1.5 added the validating binary pgrx write entry point.
The earlier multi-host CNPG evidence above used 0.1.1; it is historical evidence,
not a fresh 0.1.7 CNPG acceptance run.

For an existing cluster, roll out the rebuilt image to every PostgreSQL instance
before upgrading the extension: upgrading from before 0.1.5 needs a new C function symbol.
The 0.1.5→0.1.6→0.1.7 migrations are SQL-only and require no additional grants.
When upgrading from an older version, grant the new public entry points to
the gateway role and restart gateways to enable the binary paths:

```sql
GRANT EXECUTE ON FUNCTION public.broker_fetch_many(jsonb) TO broker_app;
GRANT EXECUTE ON FUNCTION public.broker_fetch_binary(jsonb) TO broker_app;
GRANT EXECUTE ON FUNCTION public.broker_append_bytes(text,integer,bytea) TO broker_app;
```

Without a new grant, the gateway retains the corresponding compatibility path. Internal
`broker` schema functions remain inaccessible to the gateway role. Current
manifests grant all public entry points when bootstrapping a fresh database.
