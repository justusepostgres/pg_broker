# Isolated OpenRaft compatibility probe

This separate Cargo workspace compares pinned OpenRaft 0.9.25 and
0.10.0-alpha.37. It does not change the broker workspace's dependency versions
or deploy an alpha consensus engine. Its own lockfile includes exact dependency
resolution; run with the repository's Rust 1.92.0 toolchain:

```sh
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 CARGO_BUILD_JOBS=1 \
  cargo test --locked \
  --manifest-path tests/experiments/openraft-compatibility/Cargo.toml \
  --target-dir target/openraft-format-probe -- --nocapture
```

Three tests cover nine selected old → new → old JSON fixtures and two upstream
reader-default behaviors. The fixtures include log IDs, votes, stored membership,
blank/normal/membership entries and append/retention command envelopes. The
command payload is an opaque JSON value here: this does **not** test production
`PartitionCommand` validation, PostgreSQL data, migration or network consensus.

The reader probe demonstrates that the new default `entries_stream` calls a
complete read before yielding anything. It requests all 4,097 entries at once,
so a reader capped at 4,096 still fails. Even the successful 4,096-entry case
performs the whole read before the first stream poll. The streaming API enables
an override; it does not supply bounded reads automatically.

A production migration therefore needs a custom paged stream with both entry
and byte bounds, consistency under purge/truncation/ownership changes, short
database transactions, and cancellation tests. It also needs callback durability,
snapshot installation, restart recovery and large committed-jump integration
tests before RF3 acceptance. None is established by this probe.

Primary source: the published [OpenRaft alpha crate](https://static.crates.io/crates/openraft/openraft-0.10.0-alpha.37.crate),
`src/storage/v2/raft_log_reader.rs` and `src/core/sm/worker.rs`. Source archive and
file hashes are retained in `tests/results/2026-11-04-openraft-streaming-review/`.


A custom application-stream prototype now passes six additional tests (nine
suite tests total). It streams 10,001 entries lazily in at most 128-entry/1 MiB
pages, handles byte-limited pages, validates complete page continuity before
yielding its prefix, propagates page failures, stops on cancellation, rejects
oversized entries and handles empty/u64-maximum ranges without wrapping. The
synthetic backend generates one entry at a time and does not preallocate the
whole requested range. No background task prefetches after stream drop.

This tests the upstream `entries_stream` override and paging control flow only.
It is not a PostgreSQL adapter and does not establish consistent database reads
under purge, truncation or writer replacement. Production integration must use
the existing durable owner epoch and SQL byte/entry limits, restrict application
to verified committed ranges, and test failure/recovery against PostgreSQL.
`leader_bounded_stream` also defaults to a complete range read and needs a
separate bounded implementation with vote validation for network replication.
Production dependency pins are unchanged. Source and test logs are retained in
`tests/results/2026-11-04-openraft-paged-stream/`.


Two ignored integration tests now connect the production SQL19 committed-page
cursor to the alpha `entries_stream` override. They read 10,001 persisted normal
entries in exact order, check payload preservation through the old/new envelope
conversion, verify single-slot pool availability at page boundaries, and stop
when another owner replaces the reader between pages. Run them against an
explicitly disposable SQL0.1.19 database with `DATABASE_URL` and the same Cargo
command plus `-- --ignored --test-threads=1`. Nine non-database tests and these
two integration tests pass; Clippy passes with warnings denied.

The isolated workspace now pins Tokio 1.48.0 and serde_json 1.0.154 to match its
new development dependency on the production cluster crate. Its lockfile was
updated offline; production dependency pins were not changed. This bridge
accepts only the explicitly supplied committed endpoint and rejects unsupported
complete-read and replication-vote operations. It is not a drop-in Raft storage
implementation, running state machine, snapshot installer or network transport.
Evidence is in `tests/results/2026-11-04-openraft-postgres-stream-verified/`.
The initial run used a shared server/test log filename; its mixed log is retained
in the sibling `2026-11-04-openraft-postgres-stream/` directory. The verified run
separates server and test logs.


### Real single-node alpha engine probe (2026-11-04)

The isolated compatibility workspace now runs OpenRaft 0.10.0-alpha.37 against
SQL19 through a log adapter and a streaming state machine. The application
responder is supplied by the real engine and is sent only after the PostgreSQL
application transaction confirms COMMIT. The authoritative journal payload,
partition records, membership and applied marker retain the production SQL
contract; no callback is fabricated and no database transaction waits on a client.

Four real-engine tests pass: a locked partition delays the client acknowledgment
until application commits; an invalid append returns an error without changing
the partition offset; restart replays a 10,001-entry committed application gap;
and replacement of durable ownership fences a running engine, followed by
successful recovery under a new owner. The ordinary restart case also verifies
that the durable applied ID equals the acknowledged log ID and consumption
positions advance without reapplying the acknowledged record.

Validation: nine unit/probe tests, six PostgreSQL integration tests (four engine
and two page-stream tests), and all-target Clippy with warnings denied. Evidence:
`tests/results/2026-11-04-alpha-engine-fencing/`. The initial three-engine-test
run is retained in `tests/results/2026-11-04-alpha-engine/`.

This is an experimental single-node adapter, not a production dependency upgrade.
Snapshots are disabled and snapshot operations return Unsupported; network calls
return Unreachable. Complete log reads retain the production bounded-read limit,
and the default replication stream still needs a separate vote-fenced bounded
implementation. Atomic snapshot installation, real network replication, RF3
failure survival and horizontal scaling are unverified. Engine shutdown/reopen
in these tests is orderly, not a process-kill or power-loss test. The separate
SQL19 crash tests cover committed-boundary persistence. A failed acknowledgment
may still require reconciliation after an ambiguous commit; these tests do not
establish exactly-once delivery. The production pin remains OpenRaft 0.9.25,
default SQL installation remains 0.1.7, and the live cloud benchmark remains SQL18.


### Three-engine TCP quorum probe (2026-11-04)

The isolated alpha workspace now includes a loopback TCP transport and a test
with three real OpenRaft engines, each using a separate PostgreSQL database.
After an acknowledged append, it immediately stops the leader's listener and
engine, confirms a new leader through a ReadIndex barrier, and verifies that
both survivors apply the acknowledged record. It then appends again, reopens the
old engine with a new storage epoch, verifies catch-up, and compares all three
replicas' record offsets and bytes. With two engines stopped, the remaining
engine cannot acknowledge a new write and its applied offset does not advance.

Frames are limited to 16 MiB, including during outgoing serialization; each
listener admits at most 16 concurrent requests. Connections and request work
have a two-second deadline. Server shutdown aborts and joins outstanding RPCs.
Addresses must be loopback. Snapshots explicitly fail as unsupported. These
limits and JSON encoding are experimental, not a production transport contract.

Eleven unit/probe tests and seven database integration tests pass. The strengthened
quorum test also passes three additional fresh-database runs; Clippy passes with
warnings denied. Evidence is in `tests/results/2026-11-04-alpha-quorum-ack/` and
`tests/results/2026-11-04-alpha-quorum-repeat/`. The first attempt's election-readiness
failure is retained in `2026-11-04-alpha-quorum/`: leader metrics alone were too
early, so the test now waits for ReadIndex confirmation before proposing. A
sandbox-denied socket test is retained separately; it passes with loopback access.

These tests use orderly engine shutdown, a single process and one physical
PostgreSQL server. They do not establish process-kill, independent-host RF3,
Kafka gateway routing, distributed topic identity, storage scaling or benchmark
performance. Full snapshots, production transport/security and the large-range
vote-fenced alpha replication stream remain pending. Production dependencies and
the running SQL18 cloud benchmark are unchanged.


### Abrupt PostgreSQL leader failure (2026-11-04)

The alpha quorum suite now also runs against three separate PostgreSQL 17 server
processes/data directories, capturing their distinct system identifiers and
`fsync=on`, `full_page_writes=on`, `synchronous_commit=on`. Immediately after the
first client acknowledgment, it stops the leader's PostgreSQL with
`pg_ctl -m immediate`; the other two stores remain available. It verifies
acknowledged-record survival, election and further appends, restarts the failed
server through WAL recovery, reopens the Raft engine under a new storage epoch,
and checks identical record offsets/bytes after catch-up. Minority-write refusal
is tested afterward. This is still one physical host and one Raft process: it is
not an independent-host, gateway-process-kill or power-loss test.

The repository harness `tests/experiments/openraft-compatibility/run-postgres-tests.py`
creates only disposable clusters, refuses an existing data/evidence directory,
uses loopback ports 55433–55435, and retains test logs, storage identities and
restart logs before removing its temporary data. It requires PostgreSQL 17 and
an installed SQL0.1.20 extension. Example (adjust the binary directory):

```sh
python3 tests/experiments/openraft-compatibility/run-postgres-tests.py \
  --pg-bindir /opt/homebrew/opt/postgresql@17/bin
```

This is the preferred full integration entry point: the new ignored crash test
requires three servers and its explicit crash-test environment, so a lone
`DATABASE_URL` is no longer sufficient for running all ignored probes together.
Eleven unit/probe tests, eight integration tests and Clippy pass. The checked-in
harness was itself run successfully. Evidence:
`tests/results/2026-11-04-alpha-quorum-reproducible/` (including confirmed WAL redo),
and the earlier successful `2026-11-04-alpha-quorum-postgres-crash-verified/`.
The initial failed `2026-11-04-alpha-quorum-postgres-crash/` is retained: its restart
helper omitted the test port. That harness error was corrected with explicit
restart settings before success was claimed. Production pins, gateway behavior,
and the running cloud comparison remain unchanged.
