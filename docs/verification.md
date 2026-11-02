# Verification record

Historical verification before the PG Broker rename, executed on 2026-11-02, macOS arm64, PostgreSQL 17.5, Rust 1.92.0,
pgrx 0.16.1, Python 3.11 and kafka-python 2.0.2.

- `cargo build --workspace --locked`: extension and gateway compiled and linked.
- Installed the actual shared library and versioned SQL, then `CREATE EXTENSION pg_broker` in the disposable native cluster.
- `cargo test --locked`: protocol validation test passed; gateway target compiled.
- `cargo clippy --workspace --locked -- -D warnings`: passed with the PostgreSQL 17 pg_config configured.
- Full pytest suite with the PostgreSQL crash test enabled: **21 passed in 27.24 seconds**.

The integration suite verifies:

- Real client automatic API negotiation, topic creation/list/description/deletion.
- Produce/fetch keys, headers, timestamps and partition offsets.
- Commit, SIGKILL gateway, restart, resume at the saved next offset.
- Four concurrent producers and per-producer order, plus gap-free partition offsets.
- Independent manual and subscribed groups reading the same records.
- Two separate consumer processes acquiring disjoint partition assignments; a killed member's partitions reassigned to the survivor.
- Stable group state across gateway restart, stale generation and expired-member fencing.
- Concurrent second append observed waiting on a database lock while the first is uncommitted; consumers cannot advance past it. Rollback restores offset allocation.
- Retention advances earliest without resetting latest, and old fetches are rejected.
- Multiple batches, timestamp lookup and CRC rejection with no offset advancement.
- Every advertised Metadata and classic group request version, using the target client's encoders.
- Duplicate SyncGroup response-loss retry and stale wire OffsetCommit rejection.
- A second live gateway heartbeats and commits against the first gateway's group state.
- Oversized and negative wire frame rejection.
- SQL extension upgrade preserves records and committed offsets.
- Immediate PostgreSQL shutdown (no clean checkpoint), WAL recovery, and preservation of acknowledged records and committed offsets.

Docker Compose was syntax-inspected but not run: no Docker daemon was available.
The suite does not establish full Kafka conformance, production throughput, rolling-upgrade compatibility,
or behavior under arbitrary long-running network partitions. No such guarantees are claimed.

## CNPG work in progress

The subsequent native regression passed **26 tests in 34.92 seconds**, including
confluent-kafka 2.6.1/librdkafka and a missing-synchronous-standby acknowledgment test.
ApiVersions now supports 0–3 and classic subscription metadata supports 0–3.
The COMMIT path disables PostgreSQL's server-side statement timeout and discards
the physical connection after a five-second confirmation timeout, avoiding a
false successful acknowledgment after a canceled synchronous-replication wait.

A dedicated OCI Ubuntu ARM VM was provisioned for deployment verification.
Its private Kubernetes installation is K3s v1.35.1+k3s1 with CNPG 1.30.1.
The operator is healthy and `kubectl apply --dry-run=server -k deploy/cnpg`
passes against its CRDs and admission webhook. Image builds and live acceptance
are still in progress; these facts alone do not prove deployment completion.

## PG Broker rename verification

On 2026-11-02, the renamed `pg_broker` extension and `pg-broker-gateway`
passed `cargo build --workspace --locked`, `cargo test --locked`,
workspace Clippy with warnings denied, and `cargo fmt --all -- --check`.
This build used the installed PostgreSQL 17.11 development headers; the
container baseline remains pinned to 17.5. Shell and Python syntax checks
passed, and packaging generated the renamed control, install, and upgrade SQL.
The live database integration suite and CNPG deployment were not rerun as
part of the rename; the integration results above predate it.
