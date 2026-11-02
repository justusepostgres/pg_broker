# PG Broker

A PostgreSQL-backed partitioned event log, with a **Rust/pgrx extension** and a separate **Rust Kafka TCP gateway**. Records are retained independently of consumption. Each consumer group has its own committed offsets; classic group members share partition assignments.

This is an executable MVP, not a drop-in replacement for a full Kafka cluster. The compatibility targets are **kafka-python 2.0.2** and **confluent-kafka 2.6.1 (librdkafka 2.6.1)** on Python 3.11, using its ordinary producer, consumer and admin APIs with automatic broker-version detection. The gateway presents the supported Kafka 0.11-era API subset. See [the exact compatibility matrix](docs/compatibility.md).

## Milestones

1. Manual assignment: create topic → produce → fetch → commit → kill/restart gateway → resume. Implemented and tested with the real client.
2. Classic groups: JoinGroup, leader-computed range assignments, SyncGroup, heartbeats, generation fencing, rebalance on membership changes, consumer-process failure, and gateway recovery. Implemented and tested. **Manual assignment is no longer required**, but remains available for unmanaged consumers.
3. Validation: concurrent producers, an intentionally uncommitted append, rollback, independent subscribed groups, retention boundaries, timestamp lookup, multiple gateways and SQL upgrades. Tests live in `tests/`.

## Versions and primary sources

| Component | Pin | Reason |
|---|---|---|
| PostgreSQL | 17.5 | Matches the local tested server; Docker builds this exact source release with SHA-256 verification. This is a development baseline, not a claim that 17.5 is the latest security release. |
| Rust | 1.92.0 | `rust-toolchain.toml`; tested compiler, compatible with the locked dependencies. |
| pgrx | 0.16.1 | Exact Cargo dependency, PostgreSQL 17 feature only. |
| kafka-python | 2.0.2 | Initial compatibility contract; Python 3.11 is required for this older client's import machinery. |
| Python tests | `tests/requirements.lock` | All test-environment packages pinned. |

Rust dependency versions are captured in `Cargo.lock`; use `--locked`. The implementation was checked against the [PostgreSQL locking](https://www.postgresql.org/docs/17/explicit-locking.html), [transaction isolation](https://www.postgresql.org/docs/17/transaction-iso.html), and [extension installation](https://www.postgresql.org/docs/17/sql-createextension.html) documentation; [pgrx v0.16.1 source and safety conventions](https://github.com/pgcentralfoundation/pgrx/tree/v0.16.1); the [Rust 1.92 release](https://blog.rust-lang.org/2025/12/11/Rust-1.92.0/); the [Apache Kafka protocol](https://kafka.apache.org/protocol.html); and the [pinned client's protocol definitions](https://github.com/dpkp/kafka-python/tree/2.0.2/kafka/protocol). Its [version inference](https://github.com/dpkp/kafka-python/blob/2.0.2/kafka/conn.py) and [group coordinator](https://github.com/dpkp/kafka-python/blob/2.0.2/kafka/coordinator/base.py) determine which APIs must be implemented.

## Architecture

```text
Kafka clients → TCP gateway(s) → public.broker_call(jsonb) → pgrx/SPI
                                                        → extension-owned SQL tables/functions
```

- `crates/protocol`: bounded wire primitives and magic-2 record-batch validation, including CRC32C and record framing.
- `crates/extension`: pgrx entry point, storage-boundary batch validation, and versioned SQL owning topics, partition counters, batches, group state, membership, assignments and offsets.
- `crates/gateway`: TCP connections, API translation, metadata, capability negotiation, fetch long polling, and classic-group coordination orchestration.

PostgreSQL is authoritative. The gateway has no recovery-critical local state and no partition ownership in memory. Storage uses ordinary WAL-logged heap tables and supported SPI calls. There is no custom storage engine, custom WAL or handwritten unsafe Rust. Protocol headers, key/value nullability, timestamps and record bytes are preserved. Fetch changes only the batch's base offset, which lies outside its CRC.

### Offsets, visibility and acknowledgment

Each partition has a durable `next_offset` row. Append locks it with `SELECT ... FOR UPDATE`, validates all batches, inserts a contiguous range and advances the counter **in the same transaction**. The lock survives until commit or rollback. A concurrent append on that partition cannot allocate or commit a higher range before its predecessor finishes. Rollback restores both rows and the counter. There are no sequences.

The gateway commits with `synchronous_commit=on` before returning a successful Produce response. Both `acks=1` and `acks=all` mean the PostgreSQL commit has completed; this is not a promise of Kafka replica acknowledgment. PostgreSQL's configured synchronous replication, WAL and storage durability determine the actual failure domain. `acks=0` runs the same write path but sends no response and gives the caller no durability confirmation.

Fetch reads bounds and batches in one short **REPEATABLE READ** transaction per partition. Its high watermark is the next committed offset, exclusive. Thus bounds and records come from a consistent snapshot even if retention or appends run concurrently. Fetch can include a batch that starts before the requested offset; the standard client discards earlier records. Transactions end before polling sleeps or network writes. Multiple partitions in a request are independently committed/read; cross-partition atomicity is not promised.

A lost response after commit is ambiguous. Retrying a produce can duplicate records. Consumers should process before committing the **next** offset, tolerate replay, and make external side effects duplicate-tolerant. The delivery model is **at least once** while records remain retained and clients follow this discipline. There is no exactly-once guarantee.

### Groups and coordinator recovery

All gateways represent one logical broker/coordinator ID (`1`). Put them behind the **same advertised TCP endpoint** for transparent failover. `LISTEN_ADDR` may differ per process; `ADVERTISED_HOST`/`ADVERTISED_PORT` must identify the shared endpoint. The database group row is the exclusive serialization point for every transition, heartbeat and commit. There is no gateway lease to expire or compete over: a gateway owns only its short database transaction. The multi-gateway integration test verifies shared generation and offset state.

Groups move through `empty → joining → completing → stable`. Starting a rebalance durably increments the generation and clears assignments. Members rejoin; a deterministic leader receives the subscriptions and uses the client's standard **range** assignor. SyncGroup persists all assignments atomically. The gateway validates assignment encoding and unique partition ownership; storage checks member coverage, subscribed topics and existing partitions. Identical leader SyncGroup retries are accepted, but changing a stable generation's assignment is rejected.

Heartbeats during rebalance return `REBALANCE_IN_PROGRESS` so the client retains its member identity. Stable-generation heartbeats and commits check durable membership, generation and expiry. Managed commits additionally require partition ownership. Expiry is evaluated under the group lock on the next group operation, including polling; no background gateway timer is required for recovery. Rejoin deadlines use the group's maximum observed rebalance timeout, capped at five minutes. Pending joins refresh their liveness; members that fail to rejoin by the deadline are evicted. Session timeouts are 3–300 seconds.

Unmanaged/manual commits (`generation=-1`, empty member ID) are accepted only when there are no active managed members. Kafka Fetch v4 contains no group/member credentials: fencing protects group transitions and commits, **not application processing of records already fetched**. Rebalances can therefore cause duplicate processing. Group/offset state does not expire automatically. Empty-group rows retain their generations to prevent reuse.

### Retention

Default retention is seven days, measured from **broker append time**, not a producer-controlled timestamp. Every gateway runs an idempotent sweep every 30 seconds. Each sweep transaction removes at most 4,096 batches from a partition's expired prefix and advances its durable `earliest` offset under the same partition lock used by append. Retention is batch-granular; a retained batch can contain older records. A backward wall-clock jump cannot create holes: only a contiguous prefix is removed.

`earliest` is inclusive; `latest`/high watermark is exclusive. ListOffsets `-2` and `-1` expose these boundaries. Fetch below earliest or above latest returns `OFFSET_OUT_OF_RANGE`; fetch at latest is empty. Offset commits do not pin records or move retention, and deleting records never resets offsets. Consumer groups may fall behind retention and need their configured offset-reset policy. An admin can set retention with:

```sql
UPDATE broker.topics SET retention_ms = 3600000 WHERE name = 'events';
```

## CloudNativePG

See [deployment manifests and verification steps](deploy/cnpg/README.md). The `cnpg` Docker target extends the official PostgreSQL image, while the `gateway` target runs separately with certificate-verified PostgreSQL TLS. Kafka discovery advertises a shared Service address. Live CNPG deployment verification is still in progress; see [verification status](docs/verification.md).

## Local setup (tested on macOS, PostgreSQL 17.5)

Install Rust via rustup, PostgreSQL 17.5 with server headers, a C compiler/libclang and Python 3.11. `cargo-pgrx` is not required: the workspace builds the pgrx library directly and packages explicit versioned SQL. If using pgrx CLI tooling, its version must also be `0.16.1`.

```sh
# Adjust this to your PostgreSQL 17 installation.
export PG_CONFIG=/opt/homebrew/opt/postgresql@17/bin/pg_config
scripts/dev.sh build
scripts/install-extension.sh   # writes only pg_broker artifacts into PostgreSQL's directories
scripts/dev.sh init            # isolated .dev/pgdata, loopback port 55432
python3.11 -m venv .venv
.venv/bin/pip install -r tests/requirements.lock
scripts/dev.sh gateway         # foreground, loopback port 9092
```

In another terminal:

```sh
.venv/bin/python examples/client.py
# Run again after restarting the gateway: the same group resumes its offsets.
```

The local database uses trust authentication on loopback for development. The gateway defaults to the local test DSN. Configure `DATABASE_URL` for another database. Installation needs write access to PostgreSQL's library and extension directories; it never changes other extensions. `scripts/dev.sh stop` stops the local database. Its data remains under `.dev/pgdata`.

For a restricted database service account, grant only database connection and `EXECUTE ON FUNCTION public.broker_call(jsonb)`. The function is a security definer with a fixed search path. Tables and internal functions are revoked from PUBLIC. Kafka-side authentication and authorization are not implemented; keep the endpoint on a trusted network.

### Containers

```sh
docker compose up --build
BOOTSTRAP_SERVERS=127.0.0.1:9092 .venv/bin/python examples/client.py
docker compose restart gateway
```

`Dockerfile` pins Rust and checksum-verifies PostgreSQL 17.5 source, builds the extension and gateway, and runs the database and gateway as separate non-root services. PostgreSQL data lives in the `broker-data` volume. The container setup is supplied but **was not executed in the implementation environment because the Docker daemon was unavailable**. Native build and integration tests were executed instead. Update the development password before exposing the database anywhere else.

### Tests and extension upgrades

```sh
scripts/dev.sh test
PGRX_PG_CONFIG_PATH="$PG_CONFIG" cargo clippy --workspace --locked -- -D warnings
```

To include the PostgreSQL crash test against **only the disposable native cluster**:

```sh
PG_BROKER_TEST_PGDATA="$PWD/.dev/pgdata" \
PG_BROKER_TEST_PG_CTL="$("$PG_CONFIG" --bindir)/pg_ctl" \
.venv/bin/pytest -q tests
```

The final native verification passed **21 integration tests**, including that database crash/recovery test, plus Rust tests and workspace Clippy with warnings denied. See [verification details](docs/verification.md).

Tests start/kill their own gateways on ports 19092 and 19093, create uniquely named topics/groups in the configured database, and use the unmodified client. PostgreSQL must already have `pg_broker` installed. The upgrade test creates and drops one isolated temporary database, so the test role needs `CREATEDB`. Tests deliberately kill consumer and gateway processes. Use a development database; test topics remain available for inspection.

The source installation script is `crates/extension/sql/pg_broker--0.1.0.sql`; the upgrade is `pg_broker--0.1.0--0.1.1.sql`. Packaging emits both initial and current install scripts. The default SQL version is `0.1.1`; the upgrade adds a retention index without discarding data:

```sql
ALTER EXTENSION pg_broker UPDATE TO '0.1.1';
```

Extension tables are registered for `pg_dump` data inclusion. Deploy matching gateway, extension binary and SQL artifacts together. This first release does not promise rolling upgrades across future incompatible schemas.

## Bounds and limitations

- 128 connections per gateway; 16 pooled database connections; pool wait 3 seconds. Excess TCP connections are closed. A partially supplied frame or blocked network write times out after 10 seconds; idle connections after 60 seconds.
- Request/response frames: 8 MiB; partition record set and each batch: 1 MiB; 10,000 records per batch; 1,024 headers per record; bounded array counts. Fetch data is capped at 4 MiB, with Kafka's first-batch progress exception, and long polling at 5 seconds. Group metadata is capped at 16 KiB/member and membership at 128/group. Topics are capped at 128 with 1–128 fixed partitions each.
- Database statements time out after 3 seconds and lock waits after 2 seconds. A failed database operation or malformed/unsupported request closes the connection; a client may retry. A disconnect is not proof that an earlier partition append rolled back. Expected domain failures return the Kafka error codes documented in the compatibility matrix.
- Only uncompressed magic-2/CreateTime batches, plaintext TCP, replication factor 1, classic eager range groups, fixed explicit topic names, and explicit topic creation. No wildcard-subscription compatibility guarantee. No per-topic Kafka configuration APIs. Producer ordering across retries requires client discipline; concurrent producers are ordered by the database lock, not wall-clock send order.
- Idempotent producers, Kafka transactions, log compaction, Kafka ACL compatibility, SASL/TLS, newer consumer-group protocols, static membership, cooperative/sticky assignment, broker replication, partition expansion, quotas and Kafka operational/admin API parity are deferred.
- PostgreSQL remains the scaling/failure domain; row-per-batch storage and JSON/SPI translation favor auditability over maximum throughput. Retention/metadata enumeration and group serialization need benchmarking before production use. Full broker conformance, long-duration fault injection, container execution and production operational hardening remain future work.
