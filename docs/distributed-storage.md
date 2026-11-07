# Distributed storage target

Status: architecture contract for the next implementation phase. The current
broker still uses one writable PostgreSQL storage domain; these capabilities are
not implemented or advertised yet. PostgreSQL/pgrx remains the durable engine on
each independent storage node, as selected by the user. OpenRaft 0.9.25 is pinned
for evaluation of partition consensus. The cluster crate implements an
experimental local vote and log-storage adapter with durable writer fencing.
Opt-in SQL 0.1.10 supplies journal primitives and SQL 0.1.11 supports bounded
OpenRaft readers and confirmed-commit flush callbacks. SQL 0.1.12 adds local
committed-entry application to broker partitions. The full state-machine interface,
snapshots, networking and partition routing remain incomplete; this is not
a functioning replicated storage implementation.

An opt-in SQL 0.1.8 migration now implements the first prerequisite: immutable
topic UUIDs and `broker_append_identified(topic, partition, topic_id, bytes)`.
The default extension version remains 0.1.7 and the gateway still uses its
existing entry points. This does not complete D1 or provide leader fencing.

## Experimental topic identity prerequisite

After packaging and installing the extension SQL, use an isolated development
database with `CREATE EXTENSION pg_broker VERSION '0.1.8'`, or explicitly upgrade
with `ALTER EXTENSION pg_broker UPDATE TO '0.1.8'`. Existing topics receive UUIDs;
their records and offset counters remain unchanged. A deleted and recreated name
receives a new UUID. Updating an existing UUID is rejected by a trigger.

The identified append function checks the UUID and holds a topic `FOR KEY SHARE`
lock until its transaction ends, preventing deletion between the check and the
append. Different partitions can still append concurrently. It calls the existing
pgrx batch validator and transactional offset allocator. A missing or stale UUID
returns error 3 (unknown topic/partition); null arguments return error 42; invalid
records retain the validator's error mapping. Execution is revoked from PUBLIC;
an administrator must explicitly grant it to a trusted storage caller.

Seven PostgreSQL integration cases in `tests/test_topic_identity.py` cover fresh
installation, upgrading populated 0.1.7 storage, unchanged identity across
connections, immutable IDs, stale requests, both orderings of the delete/append
race, concurrent distinct partitions, rollback and actual role permissions.
The lock follows PostgreSQL's supported [row-lock semantics](https://www.postgresql.org/docs/17/explicit-locking.html);
IDs use the built-in [UUID generator](https://www.postgresql.org/docs/17/functions-uuid.html).

Legacy name-based APIs remain available and bypass the new identity check.
Gateway routing, replica bootstrap, deletion tombstones, leader/configuration epochs, quorum commit,
and identity-aware group offsets remain unimplemented. This schema is opt-in
development work; backup/restore and deployment acceptance for 0.1.8 are pending.
It must not be used to claim stale-leader fencing or safe distributed storage.

## Experimental durable votes (SQL 0.1.9)

`broker-cluster::votes::VoteStorage` persists typed OpenRaft 0.9.25 `Vote<u64>`
values through extension-owned SQL functions and a WAL-logged table. Install
SQL 0.1.9 explicitly in a disposable database, or upgrade 0.1.8 explicitly to
0.1.9; the default extension version remains 0.1.7. The running Kafka gateway
does not use this API.

Claiming a stable replica-group key atomically increments a local writer epoch
and preserves its vote and revision. Every read and write checks that epoch.
Writes also compare the expected revision; a stale owner cannot acknowledge an
identical vote from memory or overwrite a successor's vote. The Rust adapter
rejects vote regression using the pinned OpenRaft ordering and serializes its
operations through exclusive mutable access. The SQL functions are private to
explicitly granted, trusted storage code: they store opaque JSON and do not
independently implement OpenRaft's vote-ordering rules.

Each operation uses a short transaction, checks for a writable PostgreSQL node
with fsync/full-page writes enabled, and overrides asynchronous session commits
with transaction-local synchronous commit. It returns success only after COMMIT
confirmation. Pool acquisition and database operations are bounded; a timeout,
error or cancellation discards the unconfirmed physical connection. The caller's
pool must have at most 16 connections. The writer epoch is not a lease, Raft term,
membership generation or quorum-leadership claim. A new claim deliberately fences
the old local owner; it must not be used to run competing live Raft identities.

Three Rust/PostgreSQL integration tests cover reopening with a new owner,
stale/identical-vote rejection, full-width node/term values, vote regression,
transaction-local durability overrides (observed by a database trigger), and
cancelling a write while it is blocked on a row lock. Four SQL tests cover
upgrading populated topic metadata, transactional rollback, revision checks,
permissions and bounds, and an actual PostgreSQL process crash/restart. All
passed on PostgreSQL 17.11; the seven identity tests also passed again after
making their cleanup reconnect after a crash. Logs are in
`tests/results/2026-11-03-raft-votes/`. Run the Rust cases explicitly against a
disposable SQL 0.1.9 database owned by the test administrator:

```sh
DATABASE_URL='host=127.0.0.1 port=55432 user=postgres dbname=postgres' \
  cargo test --locked -p broker-cluster --test votes_postgres -- --ignored --test-threads=1
```

This subsection covers vote persistence. The log adapter is described below;
`RaftStateMachine`, snapshots, replica transport, membership/placement and replicated records remain
to be implemented and fault-tested. Logical restore, cloning a Raft identity,
divergent PostgreSQL timelines and multi-node partitions are not covered by these
tests. Restoring an old vote table is not a safe replica-rejoin protocol; writer
epochs alone cannot fence a forked database history. No distributed durability,
RF=3 or stale-leader guarantee is advertised.

## Experimental journal primitives (SQL 0.1.10)

Fresh installation and upgrading 0.1.9 provide the private
`broker_raft_log(key, writer_epoch, operation, json)` API. It uses the same owner
row lock as vote persistence, so append, metadata changes and owner claims cannot
overtake each other. A new owner preserves existing log entries and fences the
old epoch. PUBLIC cannot execute the function or read the underlying tables.

Append accepts consecutive entries beginning at index zero or immediately after
the durable tail. Exact overlap is an idempotent retry; conflicting overlap must
be explicitly truncated first. A later invalid entry rolls back the entire call.
Committed log IDs must match durable entries and cannot regress. Truncation cannot
remove committed or purged entries. Purging atomically deletes a prefix and saves
its boundary, which remains the last log ID when no entries remain. It cannot
advance past the stored committed index. **The caller must additionally prove a
durable applied state or snapshot before purging**; SQL does not supply that proof.

Each entry is limited to 1 MiB, append requests to 256 entries/8 MiB, and read pages
to 256 entries/8 MiB including JSON array framing. Reads use inclusive index bounds
and callers continue after the last returned entry. Exact numeric indexes retain
the full unsigned 64-bit range without floating-point conversion or fractional
rounding; see PostgreSQL's [numeric types](https://www.postgresql.org/docs/17/datatype-numeric.html).
Log IDs and payloads remain opaque to SQL apart from index validation: trusted
callers must validate their typed OpenRaft representation. The experimental Rust
`JournalStorage<C>` wrapper now serializes/deserializes typed entries and log IDs,
bounds serialization to 8 MiB, and delegates both journal and vote operations to
one non-cloneable fenced owner. Every operation requires exclusive mutable access
and uses the existing short, synchronous, confirmed-commit transaction boundary.
Errors, timeouts and cancellation discard an unconfirmed physical connection.
The `RaftLogStorage` adapter is described below. Snapshot installation remains
pending. This experimental wrapper must not be used as a production Raft storage
adapter or treated as a functioning consensus node.

Seven PostgreSQL 17.11 tests cover fresh/upgrade installation, exact retries,
rollback, gap/conflict rejection, committed-prefix protection, actual owner-claim
blocking, fencing, permissions, bounds/pagination, and confirmed prefix survival
through an immediate PostgreSQL process crash. Evidence is in
`tests/results/2026-11-04-raft-log/`. The crash case requires an explicitly
disposable cluster. Default installation stays 0.1.7; no benchmark deployment
uses this schema, and snapshot recovery, power loss, forked timelines and
multi-node replication remain unverified.

Three additional Rust tests passed against SQL 0.1.10: typed blank/membership/data
round trips with vote preservation and owner fencing; cancellation of an append
blocked on the owner row, with physical connection replacement and no delayed
write; and observed synchronous-commit enforcement despite an asynchronous
session default, plus request bounds. All three existing vote tests and six
non-crash SQL journal cases passed again. The actual SQL crash test was separately
verified above. Evidence is in `tests/results/2026-11-04-raft-journal/`.

## Experimental OpenRaft reader (SQL 0.1.11)

`JournalStorage<C>::reader()` creates a cloneable, read-only `JournalReader<C>`
implementing the pinned OpenRaft `RaftLogReader` trait. Handles share the existing
pool, key and epoch; creating one does not claim another writer. Each read checks
the epoch under a shared owner-row lock, so readers can coexist while journal
writes and new owner claims wait. Reads use the same transaction bounds and
confirmed-commit/connection-discard mechanism as other local storage operations.

`try_get_log_entries` implements complete ranges, including inclusive/exclusive
and unbounded Rust range bounds without u64 wraparound. It returns every available
entry in the range or an explicit storage error if the response exceeds 4,096
entries/64 MiB. It never silently returns a prefix. `limited_get_log_entries`
returns at most 256 entries/8 MiB and errors if a nonempty requested range has no
available entry. Empty ranges still check fencing. Each result is built within
one transaction with the owner lock held, avoiding reads assembled from changing
snapshots. These are implementation resource limits, not measured replication
capacity; large complete reads must be divided by the caller.

SQL 0.1.11 provides a separate private read-only function. Explicitly granting it
does not grant append or raw-table access. Three new Rust/PostgreSQL tests cover
count/byte bounds, full versus limited behavior, cloned-reader fencing and
cancellation; a unit test covers integer-edge ranges. All six existing Rust
vote/journal tests passed again, along with six existing SQL cases and two new
upgrade/permission/shared-lock cases. Evidence is in
`tests/results/2026-11-04-raft-reader/`. The writer adapter below builds on these
readers; this is not yet a functioning replicated broker.

## Experimental OpenRaft writer adapter (SQL 0.1.11)

`JournalStorage<C>` now implements the pinned OpenRaft 0.9.25 `RaftLogStorage`
trait. Each append awaits confirmed synchronous PostgreSQL commit before invoking
`LogFlushed::log_io_completed(Ok(()))`. Errors report both callback failure and
storage failure. Writes are serialized through exclusive mutable access and the
same durable owner row; there are no detached writes or in-memory durability
acknowledgments. Cancellation retains the existing connection-discard behavior.
This favors a simple durability boundary over pipelined write throughput.

The adapter exposes vote persistence, committed boundaries, reader creation,
log state, truncation and purge. Saving `None` as the committed boundary checks
ownership and rejects erasing a nonempty durable boundary. Append consumes at
most 257 iterator items, rejects more than 256, and enforces the existing 8 MiB
serialized request and 1 MiB per-entry SQL limits. An oversized or invalid batch
fails atomically; it is never split into independently committed prefixes.
Future Raft configuration and command admission must respect these limits and
the complete-reader limit. A storage error can stop a node; these bounds are
not transparent support for arbitrary OpenRaft workloads.

Two new PostgreSQL tests use OpenRaft's public `blocking_append` helper, which
waits for the actual flush callback. They verify successful callback completion,
waiting behind a database lock, error termination without partial append,
recovery of votes/committed/purged markers, stale writer and reader fencing,
and bounded consumption of an infinite iterator. All nine prior Rust integration
tests and six non-crash SQL cases also pass. Evidence and source hashes are in
`tests/results/2026-11-04-raft-storage/`.

Purge still requires an externally established durable applied state or snapshot.
The application primitive below does not yet supply the complete state-machine/
snapshot implementation, snapshot rejoin, transport,
replica membership, placement or gateway routing yet. No running multi-node Raft
cluster, replicated broker, or RF=3 durability claim follows from this adapter.

## Experimental committed-entry application (SQL 0.1.12)

A `PartitionApplication` handle shares the journal owner's epoch. It binds a
replica-group key immutably to one existing, empty topic UUID/partition. The
binding is unique per UUID/partition and cannot be reset by rebinding. The control
plane must provision matching topic identities on independent nodes; this API
does not implement that control plane or replica bootstrap.

`apply(log_id)` reads the entry from the local durable journal, requires a saved
committed boundary, and advances the applied index consecutively from zero.
Blank entries advance the marker; membership entries save `StoredMembership`;
normal append commands include the expected Kafka offset, record-set bytes as
hex, and an append time in milliseconds. The existing pgrx validator and
identity-checked offset allocator process the bytes. Topic identity, expected
offset or record validation failures roll back the entire transaction and stop
application; a replica never silently skips divergent data.

Records, applied index, last membership and the last command response commit in
one transaction. The Rust handle enforces synchronous commit and discards
unconfirmed connections on errors/cancellation. Repeating the latest applied ID
returns its saved response without another append; old local owners are fenced
even on this retry path. This is replay protection for one replicated command,
not Kafka producer idempotence or exactly-once processing. Older applied IDs
are rejected; snapshot/recovery integration must resume after the durable marker.

Retention timestamps are overwritten inside the same transaction from the
replicated command rather than each replica's clock. Kafka keys, values, record
timestamps and headers remain encoded in the validated batch. The journal's
1 MiB per-entry JSON limit includes hex expansion. The opaque Rust
`PartitionCommand` now validates through the shared Kafka batch codec during both
construction and deserialization, caps raw record sets at 500 KiB, and rejects
CRC/format errors, unknown fields, timestamp bounds and offset overflow before a
caller proposes it. SQL revalidates at application time. This protects this typed
command path; trusted direct SQL callers can still store invalid entries. Only append, blank and
membership entries are supported here; retention decisions, topic deletion,
consumer offsets and group coordination are not replicated by this API.

Seven SQL tests cover fresh/upgrade installation, committed-prefix checks,
rollback/retry, membership, identity and offset divergence, CRC validation,
permissions, unique bindings and rejection of purging unapplied entries. A trigger
rolls back the whole purge if it would cross a bound partition's applied marker.
Two Rust tests cover typed results, confirmed
synchronous application, owner fencing and cancellation/connection replacement.
All 11 prior Rust storage tests and six prior SQL journal cases passed again.
A separate immediate PostgreSQL crash test verifies that the applied marker,
record bytes, timestamp and next offset survive together, and retry after owner
replacement cannot duplicate the append. Evidence is in
`tests/results/2026-11-04-raft-application/`.

This is a local application primitive, not the complete `RaftStateMachine` trait.
Snapshot export/installation, node bootstrap, transport and quorum operation
remain pending. Legacy gateway entry points can still write the same tables;
production replica ownership requires isolating those paths and configuring
retention through replicated decisions. Do not deploy this as a replicated broker
or infer distributed durability from its local crash test. Default SQL remains
0.1.7 and active benchmark binaries are unchanged.

## Experimental durable snapshot capture (SQL 0.1.13)

The application handle exposes `begin_snapshot`, `copy_snapshot` and
`release_snapshot`. Begin serializes with application using the owner row,
captures the applied marker/membership/last response and topic metadata, and
records the retained Kafka offset interval. An existing unfinished capture is
returned for recovery. Capturing an empty retained interval completes immediately.
This assumes the partition is exclusively maintained by the replica application;
legacy writes, independent retention decisions and metadata updates are not a
replicated control plane.

Each copy transaction processes at most 128 batches and 8 MiB of batch bytes plus
timestamp metadata. It iterates small metadata rows first, then fetches one
bounded batch at a time, avoiding a prefetched cursor full of large byte arrays.
Copied bytes and the continuation cursor commit together. New appends beyond the
captured endpoint may proceed between calls, and retries after a lost response
continue from the durable cursor. Copy rejects holes or missing source data.

A trigger pins the captured source interval against update/delete while capture
is incomplete. This includes retention and cascading topic deletion. Begin
updates the partition row, and the source trigger locks it before checking pins;
an older repeatable-read mutation therefore fails serialization instead of
missing a newly committed pin. Pins remain durable across crashes and are released
on completion or explicit abort. Retention may fail while a capture is unfinished;
operators must resume or abort interrupted captures rather than leave them pinned.
No database transaction stays open while waiting for a network client.

At most two manifests may exist per replica group. Explicit release/abort deletes
at most 128 captured batches per transaction and can resume after interruption;
callers must not release an image in use. There is no automatic disk-space
admission or cleanup policy yet. A large partition may require substantial disk
space and many short copy transactions; this is not a fixed-size in-memory image.

Seven SQL tests pass, covering fresh/upgrade paths, point-in-time contents while
new data is applied, count/byte page bounds, rollback, fencing, permission checks,
release/count limits, old repeatable-read writers, and an actual PostgreSQL crash
between capture pages. A typed Rust capture/release test and both application
regression tests pass. Evidence is in `tests/results/2026-11-04-raft-snapshot/`.
The crash case resumes a 130-batch image after 128 batches were durably copied.

This is local durable capture only. Snapshot export framing, checksums, receiving,
installation, replica bootstrap and the OpenRaft snapshot traits remain pending.
It is not yet a usable network recovery image or a replicated broker. The default
extension remains 0.1.7 and active benchmark binaries are unchanged.

## Experimental completed-image reads (SQL 0.1.14)

`PartitionApplication::read_snapshot(id, start_offset)` reads a completed captured
image in pages. Each query checks the same durable owner epoch under a shared
owner-row lock; release and owner replacement cannot race inside that query.
Only completed images are readable. Cursors must lie within the retained interval
and at batch boundaries. A nonterminal page must make progress, and EOF is explicit.
Callers advance with `next_offset`; retrying the same cursor returns the same
captured bytes until the image is explicitly released.

SQL first selects bounded metadata, then aggregates at most 128 batches and 2 MiB
of raw bytes plus timestamp metadata. The final hex/manifest JSON response is
also capped at 8 MiB. Captured append times are exported as integer Unix
microseconds. The Rust reader validates CRC/record structure, record counts,
timestamp metadata, contiguous offsets, continuation and EOF before returning.
The short database transaction has completed before a caller can send the page
over a network. Caller/network buffering and durable export-file management are
not implemented by this method.

Seven SQL tests cover fresh/upgrade installation, incomplete/released images,
permissions, owner fencing, stable bytes after source retention, count/byte
pagination, retained/empty intervals and multi-record batches preserving keys,
values, timestamps and headers. A typed Rust read test catches corrupted stored
bytes and verifies fencing, alongside both application cases and the capture
case. All four Rust/PostgreSQL cases and all-target Clippy pass. Evidence is in
`tests/results/2026-11-04-raft-snapshot-read/`.

### Portable stream framing (experimental Rust API)

`snapshot_stream::export_snapshot` sends a completed image to an asynchronous
writer, one validated page at a time. `SnapshotStream` decodes it with an 8 MiB
per-frame limit and a caller-supplied whole-stream byte quota. Version 1 consists
of the eight bytes `PGBRSN01`, then big-endian u32-length-prefixed JSON pages,
then a 32-byte SHA-256 digest after the page marked EOF. The digest covers the
magic, lengths and exact page bytes. The capture manifest must remain identical
across pages; offsets must be contiguous from earliest through the captured end.
The format encodes one image; subsequent transport bytes are outside that image.

Export waits on its sink only after the database transaction closes. Cancellation
or failure leaves partial output that must be discarded. A failed or cancelled
reader is poisoned. Earlier pages are provisional until the complete checksum
has verified and `next_page` returns None. A future installer must stage them
outside live state, authorize topic UUID/group/applied marker/membership, and
atomically publish only the complete image. SHA-256 detects corruption; it does
not authenticate the sender. Callers must set IO deadlines, concurrent-transfer
limits and disk quotas, and prevent release of the source image during export.

Unit tests cover empty/multipage images, every truncation point, checksum failure,
size limits, reordered/gapped pages, manifest changes and cancellation. A real
PostgreSQL export through a 32-byte pipe verifies round-trip data and a separate
SQL query through the single-connection pool while the export waits on IO.
Evidence: `tests/results/2026-11-04-raft-snapshot-stream/`.
Atomic installation, OpenRaft snapshot integration and network replication remain
pending. Default SQL stays 0.1.7; active benchmark artifacts remain unchanged.

### Durable file staging (experimental Rust API)

`snapshot_file::receive_snapshot_file` receives an EOF-delimited stream into a
new `incoming.partial` in a trusted private per-transfer directory. A 64 KiB copy
buffer and caller-supplied byte quota bound the download; validation then uses
one bounded snapshot page at a time. The complete image, record bytes, manifest
consistency and checksum must validate, and trailing bytes are rejected.

Publication syncs the validated file, creates a no-overwrite hard link named
`snapshot.bin`, syncs the directory, removes the partial link, and syncs the
directory again. This requires local filesystem hard-link and fsync support.
Existing files are never overwritten. Failed or cancelled downloads remain
partial for explicit cleanup; a fully received partial can be revalidated and
published after restart. A crash during publication may leave both links, and
an error after linking is ambiguous: reopen and validate the existing destination
instead of replaying blindly. File existence alone never authorizes installation.

Callers must enforce concurrent-transfer/disk quotas, transport authentication,
IO deadlines, and exclusive ownership of the staging directory. The API does
not install PostgreSQL state, authorize manifest contents, or implement the
OpenRaft snapshot traits. Tests exercise corruption/truncation, trailing bytes,
quota enforcement, cancellation, recovery from saved partials, no-overwrite
publication and a real database export/stage/reopen round trip. These tests do
not establish power-loss durability of arbitrary host filesystems.

## Required distributed behavior

Adding storage nodes must add independent writable storage and distribute
partition leaders and replicas across them. Increasing gateway replicas or
adding standby copies of the same PostgreSQL database does not meet this goal.
Replica placement, replication, failover and reassignment must operate on
individual topic partitions, rather than on an entire database cluster.

Kafka separates its controller metadata quorum from partition data replication.
Each partition has its own leader and replica set. The comparable target here is
a control plane for membership/placement plus a data plane that stores different
partition replicas on different nodes. It does not require implementing Kafka's
internal KRaft wire protocol, and a compatible API does not prove comparable
throughput or scaling efficiency.

Primary references: [Kafka controller quorum](https://kafka.apache.org/44/operations/kraft/),
[partition replication](https://kafka.apache.org/34/design/design/), and
[Kafka partition implementation](https://github.com/apache/kafka/blob/trunk/core/src/main/scala/kafka/cluster/Partition.scala).

## Current assumptions that must change

| Current implementation | Distributed replacement |
|---|---|
| One database pool in `gateway/src/server.rs` | Broker identity and partition-aware local/remote routing |
| One logical broker ID, leader and replica in `gateway/src/handlers.rs` | Real broker endpoints, per-partition leaders, replicas and in-sync status |
| CreateTopics accepts replication factor 1 only | Validated per-topic replication and minimum in-sync requirements |
| One PostgreSQL primary allocates and commits every partition offset | Independent partition leaders with durable leader epochs and replication |
| `next_offset` also serves as the committed high watermark | Distinct local log end and replicated committed high watermark |
| PostgreSQL row locks fence all writers in one database | Storage-enforced leader/configuration fencing across independent nodes |
| All group state/offsets share one database | Partitioned, replicated group-coordinator state and offsets |
| CNPG promotes a complete database | Partition-local promotion, catch-up and log reconciliation |
| Whole-database restore | Replica bootstrap, consistent shard snapshots and coordinated recovery |

The existing protocol parser, record validation, Kafka client tests and classic
consumer-group behavior remain useful. The single-storage-domain tests remain
regressions; they cannot serve as evidence for distributed correctness.

## Safety contract

1. Give topics immutable IDs and partitions explicit identities. Deletion and
   recreation of a topic name must not resurrect old replicas or offsets.
2. Persist broker incarnations, leader epochs and configuration epochs. A stale
   leader must be rejected at the durable write boundary, including after restart.
3. Allocate ordered offsets at the partition leader. Replicate record bytes with
   explicit offsets/epochs; retries must detect an identical replicated batch
   and reject conflicting contents at the same offset.
4. Track local log end separately from the committed high watermark. Consumers
   must never pass uncommitted records that could disappear or become visible
   later. Replication and its commit rule must have a written safety argument.
5. Define acknowledgment behavior for each acks setting, replication factor,
   minimum in-sync count and storage-flush policy. Never equate local PostgreSQL
   commit with distributed acknowledgment. Preserve acknowledged records across
   every failure covered by the declared replication policy.
6. Elect only eligible replicas by default. Reconcile divergent uncommitted tails
   and epoch boundaries; never truncate a committed prefix or silently elect a
   stale replica to improve availability.
7. Make placement and replica-set transitions durable and recoverable. Bootstrap
   a new replica, catch it up, commit the new configuration, then retire the old
   copy. Do not delete the last safe copy during movement or controller failure.
8. Replicate group generations, assignments and offsets with deterministic state
   transitions. Followers must not independently expire members using their own
   clocks. Shard group coordination so it does not become the next global write
   bottleneck.
9. Bound replication queues, bootstrap traffic and concurrent migrations. Apply
   backpressure when followers cannot keep up; do not accumulate an unbounded
   in-memory log or hold SQL transactions across network waits.
10. Specify retention/bootstrap interaction, deletion tombstones, disk-full
    behavior, controller unavailability and node rejoin before enabling movement.

## Implementation milestones and acceptance gates

### D1 — independent partition leaders

Run three independent storage nodes with distinct Kafka broker endpoints. A real
Kafka client creates a multi-partition topic and sends concurrently to leaders
on different nodes. Metadata returns the actual routing. A wrong-node request
returns the appropriate Kafka leader error and the client refreshes metadata.
Inspect the physical stores to prove partitions are distributed. RF=1, if used
as a development intermediate, must remain explicitly unreplicated and cannot
be called the distributed-storage milestone complete.

### D2 — replicated partition log

Run RF=3 with the configured acknowledgment policy. Kill a leader after an ack,
promote an eligible follower, and prove the acknowledged prefix and committed
offsets survive. Inject failures before/after replication and commit decisions,
partition the old leader, reject stale writes, restart nodes, and verify tail
reconciliation. Include a loss-of-required-replicas case that refuses writes.

### D3 — elastic storage membership

Start with three nodes and add a fourth. Reassign partition replicas and leaders
under active traffic, observe bytes and ownership moving, then drain/remove a
node. Preserve acknowledged data and per-partition order throughout. Crash the
controller and migration workers at every durable migration phase and resume.

### D4 — distributed groups and scaling evidence

Place consumer-group coordinators/offset storage across nodes. Verify independent
groups, member failures, rebalances and stale commits during replica movement.
Compare one versus multiple storage nodes under the same replication, durability,
record-size and client-load settings. Measure aggregate throughput, latency,
storage growth, replication traffic and recovery behavior. Report observed limits;
do not assume linear scaling from the architecture alone.

## Consequences for transactions and deployment

Multiple independent storage nodes remove the ability to use one PostgreSQL
transaction for arbitrary cross-partition output and offset commits. Kafka-style
transactions require a separate distributed transaction design with durable
coordinator decisions and transactional visibility. The existing at-least-once
contract remains until that design is implemented and fault-tested.

Each PostgreSQL data node will store only its assigned
partition replicas and the broker owns partition-level replication. CNPG can
manage PostgreSQL processes and the control-plane database, but whole-database
streaming replication is not a substitute for this partition replication model.
OpenRaft's majority quorum differs from Kafka's ISR protocol. Its acknowledgment
and recovery semantics must be documented and tested before RF=3 is advertised.

### Opt-in exclusive application ownership (0.1.15)

Migration 0.1.15 adds storage guards for Raft-bound partitions. The public bind
wrapper updates the partition row under the existing vote/topic/partition lock
order. An older repeatable-read writer therefore fails serialization instead of
missing a newly committed binding. Batch mutations and changed partition fields
require an application context tied to the current PostgreSQL transaction ID.
The private context is inserted after local-owner fencing, used only while the
existing committed-entry application runs, and removed before the call returns.
Any failure rolls back the authorization along with application changes. No
caller-settable GUC grants write authority. Public wrapper identities are retained;
implementation copies are private extension functions.

Legacy JSON, binary and identity-aware append paths remain usable for unbound
partitions. They cannot mutate a bound partition. Legacy retention and topic
removal cannot remove its data either; replicated retention/deletion commands
remain to be implemented. Snapshot capture's no-op partition update is allowed.
Database superusers and the extension owner remain trusted administrators; direct
table grants, disabling triggers and schema changes are outside this boundary.

Six real PostgreSQL tests cover fresh/upgrade installation, unchanged wrapper
identity, the three legacy append paths, unrelated partition progress, committed
apply/retry/rollback, context cleanup after failure, deletion/frontier protection,
snapshot begin, private ACLs, inherited default grants, an old repeatable-read
writer and binding blocked behind a real in-flight append. The migration removes
non-owner default grants from its private authorization table and helpers while
preserving named-role grants on the existing public wrapper. This does not add
network Raft, snapshot installation or replicated retention. Default installation
and the live benchmark remain at 0.1.7.

### Committed retention boundaries (opt-in 0.1.16)

`PartitionCommand::retain(expected_earliest, new_earliest)` records an explicit
boundary in the journal. Application requires the expected current frontier,
a forward boundary no later than the log end, and a complete batch boundary.
At most 4,096 batches can be deleted by one entry. The deletion, earliest offset,
result and applied log ID commit together under 0.1.15's private application
context. Retrying the last applied ID returns its saved result; a new local owner
can recover that result while stale owners remain fenced. The result's `offset`
field is the new earliest offset for this command.

Snapshot capture pins still reject deletion. Complete or release the capture,
then retry the same committed entry. A failed application leaves both data and
applied marker unchanged. Replicas do not choose retention from their own clocks.
Leader-side time-based selection/proposal of bounded cut points, replicated topic
deletion, network replication and snapshot installation remain pending. No
automatic retention scheduler for Raft-bound partitions is provided by this step.

Nine PostgreSQL tests cover upgrade/fresh install, commit visibility, rollback,
retry/reopen, owner fencing, invalid/divergent boundaries, snapshot pins, cuts
inside a multi-record batch, exactly 4,096 deletions and rejection of 4,097.
Twelve Rust unit tests and five PostgreSQL-backed Rust integration tests pass,
including a typed retention round trip through journal commit/application and a
new owner. All-target Clippy passes. Default deployment stays SQL 0.1.7.

Immediate-stop recovery is also tested for confirmed and uncommitted retention.
The PostgreSQL suite now has 11 passes: committed frontier/data/marker changes
survive together, while uncommitted changes roll back together. Recovery then
replays the same entry under a new local owner and rejects the stale owner.
This does not establish host-loss survival or multi-node replication.

### OpenRaft application-range integration constraint

Inspection of pinned OpenRaft 0.9.25 found that `RaftCore::apply_to_state_machine`
loads the whole newly committed range through `get_log_entries` before sending
it to the state-machine worker. Our complete reader rejects more than 4,096
entries or 64 MiB. A larger committed jump can therefore fail node application;
`max_payload_entries` bounds replication payloads, not this application read.
Do not treat the existing reader/storage unit tests as proof that arbitrary
follower catch-up is supported. Before a running RF3 integration, resolve this
through a bounded application path and test large commit jumps explicitly.
Removing the memory cap or silently returning a partial complete read is not a
correct fix. This is separate from the still-missing snapshot installer.

Primary references: [pinned core source](https://github.com/databendlabs/openraft/blob/v0.9.25/openraft/src/core/raft_core.rs)
and [0.9.25 configuration](https://docs.rs/openraft/0.9.25/openraft/struct.Config.html).

The published 0.10.0-alpha.37 source supplies a streaming `RaftStateMachine::apply`
API and its worker obtains `entries_stream(since..end)`, providing an upstream
path to bounded application. This is source inspection only: the workspace
remains pinned to 0.9.25. Before migration, verify stored vote/log/membership JSON
compatibility, callback durability, stream bounds, cancellation and large commit
jumps, then rerun recovery tests. The candidate is an alpha release, not a drop-in
stable upgrade. Archive and inspected-file hashes are preserved in
`tests/results/2026-11-04-openraft-streaming-review/`.

An isolated, separately locked probe now passes nine selected JSON round trips
between those versions (three Rust tests, including two reader probes). It covers
vote/log/membership envelopes and opaque append/retention payloads; it does not
test production command validation or persisted PostgreSQL migration. More
importantly, the candidate's default `entries_stream` still reads the whole range
before yielding. The probe reproduces failure at 4,097 entries with a 4,096-entry
cap. A migration must override this with bounded, consistent paging; simply
upgrading does not fix the application-range constraint. Reproduction instructions
are in `tests/experiments/openraft-compatibility/README.md`. Production pins remain
unchanged.


The isolated alpha compatibility workspace now has nine passing tests, including
a lazy `entries_stream` override that processes 10,001 synthetic entries in
128-entry/1 MiB pages. Cancellation during a pending page, dropped streams,
byte limits, oversized entries, discontinuity, page failure and index-edge
handling are covered; Clippy passes. This validates a possible bounded API path,
not PostgreSQL-backed catch-up. The next integration must verify committed-range
and owner-epoch checks with real SQL across pages, including purge/replacement
races. The alpha `leader_bounded_stream` default also performs a complete read;
network replication needs a separate bounded implementation with vote checks.
Neither production OpenRaft pins nor the live benchmark were changed.


### PostgreSQL committed pages (opt-in 0.1.19)

`JournalReader::committed_pages(start, end_log_id)` creates a lazy cursor whose
`next_page()` reads at most 256 entries / 8 MiB of encoded JSON. Each page takes
the durable owner's shared row lock, verifies that its end is committed and that
the exact endpoint log ID is still present, then uses the existing bounded SQL
reader. Purging a needed prefix or replacing the owner rejects the next page.
Rust checks the entire page for consecutive indices before returning any entries.
The page transaction is committed and its pool connection released before return.
Encoded byte bounds do not describe total allocator overhead during JSON decoding.

A failed or cancelled call terminates that cursor; recovery must open another
cursor from its durable applied marker. Already returned pages are not revoked
by a later owner change; application still must enforce its own durable epoch.
The committed prefix cannot be truncated through supported journal operations.
Administrative direct table mutations remain outside the storage API contract.

Five Rust/PostgreSQL reader cases pass (two new), including a 10,001-entry scan
using a single-slot pool, ownership replacement, purge and cancellation. Seven
SQL cases pass (five new), covering upgrade/fresh install, byte bounds, shared
owner locks, inherited default privileges and read-only grants. Two additional
immediate-stop cases prove confirmed committed boundaries survive while
uncommitted boundary updates roll back, with stale readers fenced after reopen.
All 12 Rust unit tests and all-target Clippy pass. Evidence is in
`tests/results/2026-11-04-committed-pages/` and
`tests/results/2026-11-04-committed-pages-crash/`.

This implements the PostgreSQL paging primitive; it does not alter OpenRaft
0.9.25's complete-read contract or yet connect the isolated alpha stream to a
running state machine. Snapshot installation, network replication, RF3 survival
and horizontal scaling tests remain pending. Default installation stays 0.1.7;
the current cloud benchmark remains on its frozen 0.1.18 artifacts.


The isolated alpha stream is now tested against the real SQL19 committed-page
cursor: a 10,001-entry range preserves log IDs and normal payloads across the
old/new envelope conversion, frees the single-slot database pool between pages,
and terminates on owner replacement. Nine unit/probe tests, two PostgreSQL
integration tests and Clippy pass. The bridge requires an explicit committed
endpoint and rejects replication-vote and complete-read operations; it is not a
complete storage adapter or running state machine. Production OpenRaft pins and
the live benchmark remain unchanged. Evidence:
`tests/results/2026-11-04-openraft-postgres-stream-verified/`.


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


### Atomic replication pages (opt-in SQL 0.1.20)

`JournalReader::replication_page` now reads up to 256 entries / 8 MiB encoded
JSON while PostgreSQL holds a shared owner-row lock. The durable vote must
exactly match the requested committed vote; otherwise it returns a typed
`LeaderChanged` with no records. Unlike committed application pages, replication
pages can contain uncommitted log entries. Each page checks the exact final log
ID, rejects a missing prefix and validates consecutive returned indices. Vote
changes, owner replacement and supported journal mutations serialize against the
page transaction. The pool connection is released before the caller receives it.

This is a page snapshot, not a leadership lease: subsequent leadership changes
cannot revoke bytes already returned. A future network stream must enforce its
vote-change contract during iteration. The alpha replication stream, network
transport and snapshot installation are still pending; neither production
OpenRaft nor default SQL versions changed. The cloud benchmark remains SQL18.

Validation: five new SQL cases plus two existing reader cases pass, covering
fresh install and upgrade, 4,097 uncommitted entries, vote changes, owner fencing,
locking, replaced endpoints, byte limits, purged prefixes and inherited grants.
One new Rust integration test traverses 5,001 uncommitted entries with a single-slot
pool and verifies typed leader changes and stale-owner rejection. All-target
Clippy passes. Evidence: `tests/results/2026-11-04-replication-pages-typed/`.


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
