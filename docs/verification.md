# Verification record

Verified on 2026-11-02 against the renamed `pg_broker` extension and
`pg-broker-gateway`. This is evidence for the documented MVP API subset, not
full Kafka conformance or production operational readiness.

## Native regression

Environment: macOS ARM64, PostgreSQL **17.11** (Homebrew), Rust **1.92.0**,
pgrx **0.16.1**, Python **3.11**, kafka-python **2.0.2**, and
confluent-kafka/librdkafka **2.6.1**.

- `cargo build --workspace --locked`: compiled the extension and gateway.
- `cargo test --locked`: passed the protocol validation test.
- `cargo clippy --workspace --locked -- -D warnings`: passed.
- `cargo fmt --all -- --check`: passed during rename verification.
- Installed the library and versioned SQL into PostgreSQL, then created
  `pg_broker` version `0.1.1` in a fresh `broker_rename_verification` database.
- Full pytest run: **28 passed in 64.61 seconds**, with both
  `PG_BROKER_TEST_PGDATA` and `PG_BROKER_TEST_PG_CTL` set to the disposable cluster.

Coverage includes:

- Public client API negotiation; topic creation/list/description/deletion.
- Keys, values, headers, timestamps and partition offsets.
- Offset commit, gateway SIGKILL, restart and resume.
- Four concurrent producers, per-producer ordering and gap-free partition offsets.
- Independent manual and subscribed consumer groups.
- Two consumer processes dividing assignments, with reassignment after one dies.
- Durable group generations, expired-member and stale-generation fencing.
- An append blocked behind an uncommitted predecessor: no skipped later-visible
  records, and rollback restores offset allocation.
- Retention boundaries, timestamp lookup, multiple batches and CRC rejection.
- Advertised Metadata/classic group versions and ApiVersions 0–2 through raw
  client encoders; flexible ApiVersions 3 through the ordinary librdkafka client.
- Duplicate SyncGroup retries, stale OffsetCommit, multiple live gateways,
  bounded frames, and SQL extension upgrade preserving records and offsets.
- Immediate PostgreSQL shutdown and WAL recovery preserving acknowledged data.
- Missing synchronous standby: Produce cannot acknowledge an unconfirmed commit.
- Real pg_dump/pg_restore into a fresh database, preserving all six tables,
  independent offsets, retained bounds and subsequent append allocation.
- TCP blackhole after transaction start: ten-second gateway deadline; readiness
  cancellation discards its physical connection and subsequent requests recover.

## Live CloudNativePG acceptance

Topology: one dedicated OCI Ubuntu 24.04 ARM VM, 4 OCPUs, 24 GiB memory,
100 GiB boot disk; **K3s v1.35.1+k3s1**, **CNPG 1.30.1**. No shared OKE resources
were modified. After verification, the temporary VM, boot disk and dedicated
network resources were removed. Build logs and final cluster output were saved
locally under `.dev/cnpg-evidence/`. The test deployed three PostgreSQL Pods and two gateway Pods in
namespace `pg-broker`, using the checked-in Kustomization and Dockerfile targets.

Actual database output:

```text
PostgreSQL 17.5 (Debian 17.5-1.pgdg120+1), aarch64
pg_broker|0.1.1
broker_app sessions: 5; bool_and(ssl): true
ANY 1 ("broker-db-2","broker-db-3","broker-db-1")
broker-db-2|quorum|streaming
broker-db-3|quorum|streaming
```

`tests/cnpg_smoke.py` ran inside the cluster with unmodified public Kafka APIs
and automatic capability negotiation:

| Check | Observed result |
|---|---|
| Build/package/bootstrap | Both images built; extension loaded by CNPG bootstrap; 3/3 database instances and 2/2 gateways ready |
| Initial cross-client round trip | kafka-python and librdkafka each consumed all 12 records from two partitions using independent subscribed groups |
| Commit/restart/resume | Both groups committed next offset 4; after gateway Deployment restart, each replayed exactly offsets 4 and 5 in both partitions |
| Database switchover | CNPG plugin promoted `broker-db-2`; both groups again resumed exactly four records through unchanged Service endpoints |
| No synchronous standby | Both standbys fenced using CNPG's annotation; zero replication connections; `synchronous_standby_names` still required one standby |
| Failed durability confirmation | Primary observed at `broker_app|SyncRep|COMMIT`; Produce returned `KafkaConnectionError` after **5.01 seconds**, with no successful acknowledgment |
| Recovery after fencing | Fences removed; cluster ready; new topic produced/consumed 12 records per client and resumed four per client |

The gateway disables PostgreSQL's server statement timeout before COMMIT and
uses a five-second confirmation timeout that discards the physical connection.
This avoids mistaking cancellation of a synchronous-replication wait for a
successful durable commit. A timed-out write remains ambiguous and may appear
later; retries can duplicate it.

Reproduction commands and the automatically restored standby-fencing test are
in [the CNPG guide](../deploy/cnpg/README.md), `scripts/cnpg-vm.sh` and
`scripts/cnpg-no-standby.sh`.

## Subsequent multi-host validation

The later [recovery and load validation](production-validation.md) tested three
separate OCI hosts, hard primary-VM power loss, a restore into a separate CNPG
cluster, and bounded throughput measurements. It also added the gateway-side
transaction deadline and cancellation-safe connection disposal.

## Limits of the initial single-host evidence

All three database Pods shared one physical VM and local-path storage. Those initial
checks establish extension deployability, process-level replication/recovery,
and the named clients' supported API paths. They do not establish availability
across host/zone loss, backup restoration, production throughput, rolling
compatibility with future schemas, or behavior under arbitrary long network
partitions. Kafka transport remains plaintext on a trusted network; PostgreSQL
connections use certificate- and hostname-verified TLS.

The Docker Compose workflow and optional local kind workflow were not run to
completion. The latter exhausted local disk before verification moved to OCI.


## Batched fetch and OpenMessaging follow-up

The gateway now fetches all requested partitions in one short REPEATABLE READ
transaction, using the extension's existing storage function and a shared byte
budget. No extension schema upgrade is required. A new real-client wire test
covers an empty leading partition, an unknown partition, exhausted total budget,
the first-nonempty-batch exception and an empty topic list.

The complete native regression suite passed **29 tests in 65.56 seconds** after
this change. Gateway Clippy passed with warnings denied; formatting and shell
syntax checks passed. The pinned OpenRaft scaffold compiles but contains no
replication implementation or distributed correctness tests.

Upstream OMB at revision `5b1fa70951a323da26bd587174b58bb2c65b0b5c`, using its
unmodified Java Kafka 3.6.1 driver, completed the RF=1 broker smoke workload on an
isolated OCI ARM VM: approximately 100 messages/s, zero publish errors and zero
final backlog. This verifies protocol interoperability, not performance parity.
The harness and comparison contract are in [benchmarks/omb](../benchmarks/omb/README.md).


Extension 0.1.2 adds a primary-key seek to the fetch predicate and commit-time
`pg_notify` hints. The gateway's dedicated listener reconnects after failures,
checks for primary demotion, and publishes only bounded topic/partition events.
Fetches subscribe before their snapshot and keep the existing polling fallback.
The complete native suite passed **31 tests in 67.78 seconds**, including upgrade
preservation, inside-batch seeks, retention-prefix boundaries, absence of notices
before commit or after rollback, and listener termination/reconnection. Gateway
Clippy passed with warnings denied. The native test cluster was stopped afterward.


After removing the redundant original payload from the validated SPI request,
the rebuilt extension and gateway passed **31 tests in 67.34 seconds** against a
fresh `broker_perf_012_verification` database. A preceding repeat hit the old
disposable database's intentional 128-topic limit (30 tests passed and topic
creation failed); no broker limit was relaxed. Workspace Clippy with warnings
denied and both CNPG Kustomize renders also passed. Cloud measurements must be
read with their recorded extension and binary versions; the earlier runs do not
include this payload-copy change.


## Extension 0.1.3 SQL fetch path

The 0.1.3 upgrade adds bounded bulk reads inside the extension and a separately
permissioned SQL-only read entry point. The gateway detects installed capability
and execution permission at startup; older extensions and existing grants retain
compatibility paths. Input operations are forced to fetch; private write dispatch
remains revoked from application roles.

The complete suite passed **33 tests in 68.23 seconds** in a fresh
`broker_perf_013_verification` database. Additional tests cover attempted write
opcodes inside a fetch, invalid/oversized request arrays, restricted-role denial,
SPI fallback and activation after granting the read function. Gateway Clippy and
formatting passed. The disposable native PostgreSQL server was stopped afterward.


The 0.1.3 OCI RF=1 OMB run completed 30 ten-second samples at approximately
10,006 messages/s: zero publish errors, 44 ms p99 end-to-end latency, and 116
messages in the final sampled backlog. The previous 0.1.2 run measured 60 ms;
Kafka measured 10 ms. These are single trials, not performance parity or a
distributed-replication test. See the complete benchmark report and raw results.
Workspace Clippy passed with the documented PGRX_PG_CONFIG_PATH set; an initial
invocation without it failed because this environment has no PGRX_HOME. Both
CNPG Kustomize manifests, formatting, and diff whitespace checks passed.

The follow-up fixed 20k offered-load run completed all 30 samples but achieved
only about 13,995 published messages/s, with 3.56 s p99 end-to-end latency and
reported producer-delay p99 near 60 s. It therefore fails the sustainable-rate
gate despite zero publish errors and zero final consumer backlog. Raw results
and limitations are preserved alongside the 10k run; 32k was not attempted.


## Bounded parallel produce

The gateway now allows four independent partitions per produce request to commit
concurrently using the existing bounded pool. Repeated topic/partition entries
remain in wire order; responses retain request order and await all results. The
full request is parsed and bounded to 4,096 entries before writes begin. A failed
or cancelled request aborts its remaining tasks; unconfirmed database connections
are discarded by the existing transaction guard. Cross-partition atomicity and
exactly-once retries remain unsupported.

The native suite passed **35 tests in 72.66 seconds**. A locked-partition test
proves an independent partition commits while the response remains pending, then
checks repeated-partition offsets/values and mixed CRC/unknown-partition errors.
A limit test proves a 4,097-entry request is rejected without committing its valid
first batch. Existing crash, fencing, offset, retention, and synchronous-standby
tests also passed. Workspace Clippy with warnings denied and formatting passed.
The native PostgreSQL cluster was cleanly stopped afterward. Performance results
for this candidate must be evaluated separately from the serial-produce 0.1.3 run.


The parallel-produce OCI candidate completed both 30-sample OMB runs. At 10k,
p99 end-to-end latency was 39 ms (prior serial 0.1.3: 44 ms; Kafka: 10 ms). At
20k it achieved about 20,014 msg/s for both production and consumption, but
transient backlog peaked at 50,238 and p99 end-to-end latency regressed to
5.046 s. Both had zero publish errors and no client log errors. This verifies
higher offered-rate throughput, not acceptable latency or performance parity.
Post-run storage/read/write diagnostic SQL executed successfully after traffic
stopped; the write diagnostic rolled back its temporary topic and records.
Deployed gateway hashes match the tested source. Raw results and profiles are
in the benchmark report.


## Extension 0.1.4 binary fetch

The new permissioned SQL function returns a status row per requested partition
and raw bytea batch rows under a shared 4 MiB budget. The gateway sorts by request
index and batch offset, uses one REPEATABLE READ transaction, and keeps commit
confirmation/cancellation guards. Old extensions and existing roles retain their
JSON/SPI fallback. Fixed work_mem settings bound sort/materialization operations;
request counts, per-partition data limits and first-batch progress are unchanged.

The final native suite passed **37 tests in 201.91 seconds**, including binary/JSON
equivalence for inside-batch offsets, unknown/out-of-range partitions and shared
budgets; malformed/oversized SQL input; attempted write opcodes; restricted-role
grants and fallback; SQL upgrades; and existing crash/fencing/retention tests.
An initial run passed 36 tests but timed out a deliberately blocked append while
the test initialized clients inside its two-second lock window. Client setup now
precedes that window; lock timeout and the no-skipped-record assertions were not
relaxed. The final suite uses the explicit gateway row ordering. Workspace Clippy
with warnings denied, formatting and both CNPG Kustomize renders passed.

Matched OMB runs for 0.1.4 completed all 30 samples with zero publish errors and
clean client logs. The 10k p99 end-to-end latency was 30 ms (previous candidate:
39 ms; Kafka reference: 10 ms). At 20k the broker published and consumed about
20,013 msg/s, with 88 ms p99 latency, peak backlog 506 and zero final sampled
backlog. The previous JSON-read candidate had 5.046 s p99 and peak backlog 50,238.
These single trials show progress, not parity; repeated and RF=3 tests remain.
A post-run database restart verified durable settings and contiguous logs/counters
for all 16 partitions. Deployed source, SQL and dependency hashes match locally.
Earlier 0.1.3 results are retained separately from the binary implementation.


## Extension 0.1.5 binary append

The new pgrx entry point validates the entire record set before any write, then
passes bytea batches and derived metadata to a private SQL helper. All batches
share the caller's transaction and partition lock. Gateway size checks remain;
CRC/record validation now occurs once at the authoritative storage boundary.
Older extensions and roles without the new grant retain JSON/SPI writes.

The full native suite passed **40 tests in 71.98 seconds**. New coverage rejects
a malformed second batch before any write, verifies rollback leaves no offset
gap, and injects a database rejection on the second valid batch to prove the
first insert and counter update roll back. Restricted-role tests exercise the
gateway fallback and explicit binary-write grant. Existing concurrent-producer,
no-skipped-record, synchronous-commit, crash/recovery, and group tests passed.
Workspace Clippy, formatting, and both CNPG Kustomize renders passed. The native
test cluster is stopped. Both 0.1.5 OMB runs completed 30 samples with zero publish
errors and clean client logs. At 10k, end-to-end p99 was 16 ms (previous: 30 ms;
Kafka reference: 10 ms). At 20k it was 20 ms (previous: 88 ms), with about 20,024
messages/s and final sampled backlog 106. These single trials remain outside
full parity acceptance. Post-run checks verified all 16 durable log prefixes and
counters, durability settings, permissions, and deployed source/dependency hashes.


## Prepared statement reuse candidate (extension 0.1.5)

Gateway queries now use deadpool-postgres 0.14.1's per-connection statement cache.
Only fixed SQL strings enter the cache; request values remain bound parameters.
Transaction isolation, primary checks, connection limits, synchronous commit and
cancellation/commit-confirmation guards are unchanged. Discarded connections take
their prepared statements with them. No extension SQL or wire API changed.

The fresh-database native suite passed **40 tests in 71.54 seconds**, including
connection loss, cancellation, PostgreSQL restart, synchronous-replication timeout,
legacy function fallback and restricted-role paths. Workspace build, ordinary
workspace Clippy with warnings denied, and the protocol unit test passed. An
additional all-target Clippy check found the pre-existing test-module placement
lint in crates/protocol/src/lib.rs; it is not a new candidate diagnostic. The
first integration attempt used the wrong local PostgreSQL port and failed setup;
the recorded successful run followed correction to port 55432. Native PostgreSQL
is stopped. Matched OMB measurement completed all 30 samples at about 10,007 messages/s,
with zero publish errors and clean client logs. End-to-end p99 was 15 ms versus
16 ms in the previous trial and 10 ms for Kafka. This small single-trial change
does not establish a repeatable gain or parity. Post-run checks verified the
16 durable partition ranges and counters; deployed source hashes matched.

## Paired OMB evidence comparison

The comparison utility passed five regression tests covering mismatched driver,
workload and revision files; repeated input paths; incomplete samples; invalid
latency; preservation of failed offered rates and errors; and latency ratios and
backlog trends. It was also run against the retained real 10k Kafka and cached015
broker sidecars/logs: configurations matched and the reported p99 ratio was 1.5.
It does not declare sustainable capacity, parity, or distributed acceptance.


The matched 32k pair completed all 30 samples for each system, with zero publish
errors and clean client logs. Broker/Kafka end-to-end p99 was 27/12 ms (2.25x),
while both published/consumed approximately 32k messages/s. Broker final sampled
backlog was 146. Post-restart checks passed for all 16 partition logs and durable
settings. See `benchmarks/omb/results/2026-11-02/paired32k-1/`. The post-run
temporary TOAST diagnostic checked all 100 copied batches for byte equality and
rolled back successfully; it does not establish a durable throughput improvement.


## LZ4 candidate correctness

The full native suite passed **41 tests in 72.91 seconds**, with
`PG_BROKER_REQUIRE_LZ4=1` on PostgreSQL 17.11. The new test stores large PGLZ and
LZ4 batches in the same partition, confirms their physical compression, verifies
keys/values/timestamps/empty and duplicate headers through kafka-python 2.0.2,
rolls back an append without an offset gap, then immediately stops PostgreSQL
and resumes from a committed offset after recovery. The original column setting
is restored before the crash. There were no skipped tests. The first attempt
passed the 40 existing tests but used a null header value that kafka-python
rejects before transmission; the fixture was corrected to supported headers.
The native database is stopped. The cloud candidate image built successfully as
`sha256:6821d31236114005fd88bc092f63721b540538169b8f048e09771ed68e24da19`.
Matched PGLZ/LZ4 performance validation remains pending; native correctness is not a cloud
performance result. Docker enables PostgreSQL LZ4 support without changing its
default compression setting.


The comparison utility now supports explicit reference/candidate file labels
for PGLZ/LZ4 experiments. Six regression tests passed, including ratio direction
and invalid labels; reprocessing the retained real 32k Kafka/broker evidence
kept its 2.25 end-to-end p99 ratio unchanged.


## Ordered transaction-settings pipeline candidate

The gateway now polls transaction settings before query work in a biased Tokio
try_join. The pinned tokio-postgres 0.7.15 batch_execute implementation enqueues
its request before its first await, so the server executes settings first while
the gateway can send query work without awaiting the settings response. The
settings/query pipeline never includes COMMIT: disabling the server statement
timeout and awaiting commit confirmation remain separate, unchanged steps.
Any branch error or external cancellation still discards the connection.

This follows the driver's documented [request ordering and pipelining](https://docs.rs/tokio-postgres/0.7.15/tokio_postgres/#behavior) and Tokio 1.48's
[biased polling order](https://docs.rs/tokio/1.48.0/tokio/macro.try_join.html).
The full native suite passed **42 tests in 75.39 seconds**, with LZ4 required.
A new real-client test starts connections with synchronous_commit=off and
contrary timeouts, then checks all three required settings in a BEFORE INSERT
trigger for cold and cached append queries. Existing blackhole/cancellation,
missing synchronous standby, ordering, mixed compression and crash tests passed.
Workspace build, Clippy with warnings denied and formatting passed. Native
PostgreSQL is stopped. This candidate was not deployed into the ongoing
compression experiment; no performance gain is claimed for pipelining yet.


The completed same-image compression pair sustained about 32k messages/s for
both variants with zero publish errors and clean client logs. PGLZ/LZ4
end-to-end p99 was 44/20 ms; both post-restart storage and actual-compression
checks passed. LZ4 still exceeded Kafka's separate 12 ms reference and had a
final sampled backlog of 354. One pair does not establish repeatability or
parity, and no storage default changed. Raw evidence is retained under
`benchmarks/omb/results/2026-11-02/compression32k-1/`.

The separate pipeline gateway release build completed in 7.65 seconds using
the cached LZ4 builder. Deployed gateway source hashes match the native-tested
source. Binary SHA-256: `4c394f8a49e5a197600ef00a456e0f5d34d04939885a03d90d1fb1db33e4d8fc`. It has not been performance-tested yet.


The matched 32k baseline/pipeline gateway pair completed 30 samples each with
zero publish errors and clean logs. End-to-end p99 was 20/19 ms, while both
published/consumed about 32k messages/s. Candidate peak/final sampled backlog
was 833/226 versus 360/0 for the control. This small single-trial latency
difference does not establish a repeatable improvement or parity. Durable
partition-range and LZ4 checks passed after restart. Post-run read-only database
and retention diagnostics executed successfully; raw results and limits are
in `benchmarks/omb/results/2026-11-03/pipeline32k-1/`.


### Retention early exit (SQL 0.1.6)

The full native suite passed **45 tests in 75.63 seconds**, with LZ4 required.
Three new tests cover empty/live no-ops without frontier tuple rewrites,
missing/null/negative duration rejection, backward-clock suffix preservation,
transaction rollback, committed offsets surviving retention, the 4,096-batch
page boundary, and subsequent appends retaining monotonic offsets. The upgrade
test now traverses through 0.1.6 and verifies records and group offsets remain.
Workspace build, formatting, Clippy with warnings denied, both CNPG Kustomize
renders, and six benchmark-comparison tests passed. Native PostgreSQL is stopped.
The migration changes only retention validation and its no-op path; expired
prefix deletion retains the existing bound and partition row lock. This is not
a fresh CNPG HA acceptance run, and no latency gain is inferred from correctness.


The matched SQL 0.1.5/0.1.6 32k pair then completed with 30 samples each,
zero publish errors and clean client logs. End-to-end p99 was 18/17 ms;
both ended with zero sampled backlog. Post-restart durable offsets and actual
LZ4 checks passed. The exact migration and runner match local sources. This
single-trial difference does not meet the separate 12 ms Kafka reference or
prove repeatability. Full evidence and rollback-only post-run diagnostics are
in `benchmarks/omb/results/2026-11-03/retention32k-1/`.


### Caught-up binary fetch (SQL 0.1.7)

The full native suite passed **46 tests in 76.64 seconds**, with LZ4 required.
The new test holds a repeatable-read snapshot at the old high watermark while
another session appends and retains the entire log. The old tail stays empty,
the snapshot still sees its earlier record, and a new snapshot sees the advanced
retention boundary. A later append remains readable at the same requested offset.
The upgrade test now traverses through 0.1.7 and verifies an empty binary tail,
existing records and group offsets. Existing real-client long-poll, fencing,
rollback and database crash tests passed. Build, formatting, Clippy with warnings
denied and both CNPG Kustomize renders passed. Native PostgreSQL is stopped.

A local before/after SQL diagnostic used the same 16-partition, 16,000-batch
fixture. Median warm calls (0.1.6→0.1.7): all caught up 0.440→0.190 ms; half caught
up 0.468→0.346 ms; all with data 0.484→0.498 ms. Buffer hits were 112→16, 112→64
and 112→112 respectively. These sequential native measurements do not establish
Kafka end-to-end improvement or repeatability. Raw samples/plans are in
`benchmarks/omb/results/2026-11-03/native-tail017-profile.json`.
No OCI/Kafka trial of 0.1.7 has run yet; the `tail` runner mode prepares that
matched experiment. The prior 17 ms result belongs to SQL 0.1.6.


The subsequent SQL 0.1.6/0.1.7 OCI pair completed with 30 samples per variant,
zero client warnings/publish errors, and successful restart/contiguity/LZ4 checks.
The 0.1.7 candidate sustained about 32k messages/s with 12 ms end-to-end p99,
9.242 ms publish p99 and final sampled backlog 158. The baseline had unexplained
latency spikes and 307.001 ms end-to-end p99; all samples and the post-spike host
snapshot are retained. Do not infer a 25x causal speedup or parity from that
noisy comparison. Fresh repeated Kafka pairs, capacity and RF=3/scaling remain
unverified. See `benchmarks/omb/results/2026-11-03/tail32k-1/`.


### Comparison-evidence checks during the 0.1.7 repetitions

Nine benchmark-analysis tests pass. The opt-in `--verify-environment` mode
validates the captured RF=1 resource/durability/storage/restart contract and
checks deployment/environment consistency across trials. Regressions cover
missing or contradictory artifacts, reused store identities and mixed volume
sizes; a valid environment still does not accept a failing latency ratio.
No running benchmark scripts or workloads were changed for this local analysis
addition. The first fresh Kafka baseline completed and its restart verification
was observed running; the three-pair experiment is still active.


The first fresh 160-GiB Kafka/0.1.7 pair completed and passed captured-environment
validation. Both 30-sample runs sustained about 32k messages/s without publish
errors or client warnings. Kafka/broker end-to-end p99 was 14/13 ms; final sampled
backlog was 0/154. Exact deployed migrations, verifier and runner match the local
sources. Raw evidence is in `benchmarks/omb/results/2026-11-03/confirm32k-1/`.
Two repetitions remain; capacity and distributed/RF=3 acceptance remain open.


All three fresh RF=1 32k comparisons are now complete and environment-validated.
Kafka/broker end-to-end p99 was 14/13, 13/12 and 13/12 ms. Each system sustained
approximately 32k messages/s with zero measured publish errors. Kafka's four
startup WARN lines remain in the raw logs; none occurred after measurement
started. Every restart check passed, and deployment sources matched. This meets
the fixed-load latency target across three trials, not maximum capacity or
RF=3/distributed scaling acceptance. The 50k, 15-minute-per-system pair is the
next running experiment; native PostgreSQL remains stopped.


Review of the same completed 32k raw data found a rarer-tail limitation: broker
end-to-end p99.9 was 61/60/61 ms versus Kafka 82/29/28 ms. The successful p99
comparison is not general latency parity. Analysis now preserves p99.9,
p99.99 and maximum values with correct units and explicit missing metrics;
ten analysis tests pass. This is local analysis only; the active 50k workload
and its remote scripts were not changed.

### Deadline diagnostics prepared after the 50k broker phase

The completed broker phase sustained 50,019.6 publish / 50,019.4 consume msg/s
over 90 samples with no publish errors, but end-to-end p99/p99.9 were
40/304.001 ms. Restart storage checks passed. Interim raw artifacts are in
`benchmarks/omb/results/2026-11-03/capacity50k-broker-interim/`; the matching
Kafka phase is still running. The gateway emitted 18 untimestamped connection
deadlines, which cannot by themselves distinguish idle headers from request
failures. They are preserved rather than counted as a clean server log.

Local gateway diagnostics now include Unix-millisecond timestamps and separate
frame-length, payload, handler and response-write deadline context. Handler
failures include numeric API/version/correlation identifiers, not record data.
Timeout durations, protocol responses and transaction behavior are unchanged.
`cargo check --locked -p pg-broker-gateway`, formatting and gateway Clippy with
warnings denied passed. This diagnostic change is not deployed: the ongoing
50k pair and planned GC diagnostic continue using the previously verified
pipeline binary. A new build must be identified separately before comparing it.

Optional maintenance tracing (`PG_BROKER_TRACE_RETENTION=1`) was then verified
against a fresh local SQL 0.1.7 database with two empty partitions. A real partial
TCP frame produced the timestamped 10-second payload deadline; the scheduled
retention cycle emitted start/end timestamps, `completed_partitions=2` and
`success=true` (10,566 microseconds in this diagnostic). This validates logging,
not benchmark latency or causation. The temporary database was dropped and the
local PostgreSQL instance stopped. Gateway build and Clippy passed; cloud
deployment and the ongoing capacity pair remain unchanged.

The 50k pair subsequently completed with both 90-sample results, zero publish
errors/client warnings, and successful restart/environment validation. Captured
runner, migrations and verifier matched local sources. Kafka/broker end-to-end
p99 was 41/40 ms, but p99.9 was 208/304.001 ms. This single fixed-load trial
meets the p99 threshold without establishing repeatability, maximum capacity or
distributed/RF=3 acceptance. See `capacity50k-1/` and
`capacity50k-comparison.json` in the November 3 benchmark results.

After confirming idle services, only the disposable Kafka data directory from
the fully captured first 32k confirmation trial was reclaimed (about 12 GiB).
All evidence and restart offsets remain retained. Java 17 accepted the bounded
GC/safepoint logging configuration in a preflight check; a separately labelled
32k diagnostic pair was then launched with the original pinned broker binary.
The new local gateway diagnostics are not included in this run.

The completed GC diagnostic found broker client safepoints no longer than
4.768 ms, versus 84 ms maximum end-to-end latency; its worst window's safepoint
maximum was 1.858 ms. Both 30-sample runs and restart checks passed. Raw evidence
and aligned windows are in `benchmarks/omb/results/2026-11-03/gc32k-1/`.

A separate Linux tracing gateway built successfully from the locally checked
source (44.35s, container exit 0). Its SHA-256 is
`9ac61469507464d8a6f3ba54c667bbad0bb8b2dc8c31ef94c23c845284082727`;
the copied build-container gateway source matches the local source hash.
The broker-only `oci-server-profile.sh` diagnostic is now running with this
binary, maintenance tracing, slow SQL logs, I/O/WAL timing and client GC logs.
The diagnostic SQL was first exercised on a fresh local PostgreSQL 17 instance,
including an actual slow statement and statistics queries; that instance was
stopped and removed. Cloud settings were read back and validated before warmup.
No performance acceptance is inferred from this instrumentation.

The server diagnostic completed: all 46 SQL statements logged above 10 ms
were COMMIT (42 in the measured period), with >40 ms clusters about 21.114
seconds apart. Post-run settings confirmed 16 MiB WAL segments and
`wal_init_zero=on`; the observed WAL rate is consistent with that periodicity,
but does not prove segment initialization caused the stalls. The run retained
32k throughput and passed restart/storage checks. Full evidence is in
`benchmarks/omb/results/2026-11-03/server32k-1/`.

A controlled fresh-store on/off diagnostic is now running under
`omb-wal-init-017-1.service`. Both phases use the same tracing binary, 32k client,
logging and resource limits. The runner reads back WAL and durability settings,
then after each workload kills PostgreSQL with SIGKILL and verifies identical
partition endpoints and contiguous stored ranges after recovery. This process
crash test does not cover host/power loss. No production setting has changed;
the completed pair is required before interpreting the hypothesis.

The first on/off pair completed and passed configuration and crash-redo checks.
End-to-end p99.9 fell from 59 to 33 ms and >40 ms measured commits from 24 to 0;
publish p99 rose from 9.562 to 10.336 ms and >10 ms commits increased from 46
to 65. The opposite-order repeat is now running on fresh stores.

All 46 native integration tests passed in 77.46 seconds with `wal_init_zero=off`
on a fresh PostgreSQL 17.11 cluster, including the real-client and crash tests.
Initial/final settings confirmed fsync, full-page writes and synchronous commits
remained enabled. The test cluster was stopped and removed. Evidence is in
`benchmarks/omb/results/2026-11-03/waloff-native/`. Local uninstrumented runners
now explicitly select/capture WAL initialization mode; 16 analysis tests pass.
The completed 50k Kafka scratch store was reclaimed after its retained artifacts
passed checksum and environment verification, providing space for fresh runs.

### Opt-in topic identity prerequisite (2026-11-03 UTC)

All **53 native integration tests passed in 77.35 seconds** on a fresh PostgreSQL
17.11 cluster: the existing 46 cases on default SQL 0.1.7 plus seven identity
cases in isolated databases using fresh/upgrade SQL 0.1.8. These cover both
orderings of the delete/append race, stale UUID rejection, concurrent partitions,
rollback and permissions. This does not certify the entire gateway suite against
0.1.8 or implement partition replication. The temporary cluster was stopped and
removed; fsync, full-page writes and synchronous commits remained enabled, with
WAL initialization zeroing disabled. Logs, settings and source hashes are in
`tests/results/2026-11-03-topic-identity/`. Default installation remains 0.1.7;
0.1.8 backup/restore and deployment acceptance are pending.

### Experimental vote persistence (2026-11-03 UTC)

Three Rust/PostgreSQL integration tests and eleven SQL/identity cases passed on
a disposable PostgreSQL 17.11 instance with opt-in SQL 0.1.9. The new cases cover
local writer fencing, vote ordering, cancellation during a blocked write, effective
synchronous-commit overrides, migration, permissions, bounds and an actual process
crash. Cluster-crate Clippy passed for all targets with warnings denied. The
disposable cluster was stopped and removed. Evidence and source hashes are in
`tests/results/2026-11-03-raft-votes/`. This verifies vote persistence only; it
does not implement or certify Raft log replication, membership, snapshots,
partition routing, restored replica identities or multi-node failover. Default
SQL remains 0.1.7, and no running benchmark artifact was replaced.

### Batched offset commits (2026-11-03 UTC)

The gateway now executes one Kafka OffsetCommit request in one short PostgreSQL
transaction, using the existing extension API for every entry's generation,
membership and ownership checks. It parses the whole request first and bounds
both partition count and expanded strings. Real-client encoder tests cover
duplicate partition order, mixed valid/invalid entries, shared transaction IDs,
malformed requests without partial writes, and rollback when a later entry raises
a database error. The existing managed-group fencing tests exercise the same
batched path. Evidence and source hashes are recorded in
`tests/results/2026-11-03-offset-commit-batching/`.

The final native PostgreSQL 17.11 run passed 62 tests in 81.12 seconds; one
opt-in vote crash case was skipped (separately verified above). Default SQL
remained 0.1.7. Gateway Clippy with warnings denied and gateway/protocol Rust
tests passed. The disposable database was stopped and removed. An initial test
encoder lifetime failure is retained alongside the successful rerun evidence.
An additional managed-group batch case verifies owned/unowned partitions in one
request, stale generations across every partition, and rejection after LeaveGroup
without changing previously committed offsets. The targeted offset/group wire
suite passed ten cases in 4.30 seconds; its separate logs and source hashes are
in the same evidence directory. Production code and the live benchmark binary
were unchanged by this supplementary test.

This addresses repeated database transactions observed in the OffsetCommit path.
The initial 75k comparison used the previous binary. Its broker phase averaged about 75,162 published records/s,
but had offset-commit timeouts and 712 ms end-to-end p99; those measurements do
not establish reliable 75k capacity or identify the timeouts' sole cause.

### Experimental journal SQL (2026-11-04 UTC)

Seven tests passed in 1.35 seconds on disposable PostgreSQL 17.11 databases with
opt-in SQL 0.1.10, covering fresh/upgrade installation, atomic append and gap
rejection, committed-prefix protection, owner-claim serialization, fencing,
permissions, bounded pagination and actual process-crash recovery. Logs, settings
and source hashes are in `tests/results/2026-11-04-raft-log/`. The temporary
cluster was stopped and removed; default SQL remained 0.1.7. This verifies local
journal SQL only, not a complete Raft storage adapter, applied state, snapshot
installation, network replication or distributed durability. The running OMB
binary and schema were unchanged.

### Typed journal wrapper (2026-11-04 UTC)

Three Rust journal tests and all three existing vote tests passed against SQL
0.1.10 on disposable PostgreSQL 17.11. Six non-crash SQL journal cases also
passed; the SQL crash case was verified separately above. Tests cover typed
blank/membership/data entries, vote/log owner fencing, committed-prefix state,
cancellation while blocked on the owner row, physical connection replacement,
transaction-local synchronous commit and bounded requests. Cluster-crate Clippy
passed for all targets with warnings denied. Evidence and source hashes are in
`tests/results/2026-11-04-raft-journal/`, including the initial test-configuration
compile error and corrected run. The wrapper is not the complete RaftLogStorage
trait or a replicated storage implementation. No benchmark artifact changed.

### Bounded OpenRaft reader (2026-11-04 UTC)

Nine Rust/PostgreSQL integration tests passed on opt-in SQL 0.1.11: three reader
cases and all six prior journal/vote cases. Six existing SQL tests and two new
upgrade/permission/shared-lock cases also passed. A unit test covers unsigned
range-bound edges; all-target Clippy passed with warnings denied. The new tests
exercise both byte and count limits, including complete-read rejection instead
of silent truncation, clone fencing, and blocked-read cancellation with physical
connection replacement. Evidence is in `tests/results/2026-11-04-raft-reader/`.
This implements RaftLogReader only; write callbacks, state-machine application,
snapshot installation and network replication remain pending. Default SQL and
all active OMB artifacts remain unchanged.

### OpenRaft writer adapter (2026-11-04 UTC)

The SQL 0.1.11-backed `RaftLogStorage` adapter passed two new integration tests
using OpenRaft's real flush-callback helper. All nine existing Rust database
tests and six non-crash SQL journal cases passed again (11 Rust + 6 SQL).
The tests cover blocking until database writes can complete, callback success,
failed append rollback, bounded iterators, persisted prefix recovery and fencing.
Clippy passed with warnings denied. Evidence is in
`tests/results/2026-11-04-raft-storage/`. The temporary PostgreSQL 17.11 cluster
was stopped and removed. This adds local storage trait integration, not a state
machine, snapshots or network replication. Default SQL and benchmark binaries
remain unchanged.

### Completed offset-batching comparison (2026-11-04 UTC)

The candidate 320 GiB pair completed with both restart checks and full environment
validation. Broker/Kafka published approximately 75,054/75,044 records/s over
90 windows each, with no publish errors or logged offset-commit failures.
End-to-end p99 was 677/725 ms; p99.9 was 966/1,104 ms. Broker scheduling-delay
p99 was 3.018 seconds versus Kafka's 0.334 seconds, so similar average throughput
and message latency alone do not establish sustainable capacity. Raw evidence
and the full report are in `benchmarks/omb/results/2026-11-03/commitbatch75k-1/`
and `commitbatch75k-comparison.json` alongside that directory. This is one RF=1
trial, with different local acknowledgment durability policies, not parity proof.

### Benchmark diagnostic evidence (2026-11-04 UTC)

The benchmark analyzer now preserves gateway deadline phases, unknown diagnostics
and missing-log status. Forty-five analysis tests pass, including integration of
the new field into paired reports. Re-analysis of the completed batching pair
classifies all 18 gateway messages as frame-header deadlines; it does not infer
that those sockets were idle. Original raw checksums remain valid. Historical
OCI metrics show 6,197 throttled I/Os over 13 checked Kafka minutes versus zero
in 13 broker minutes, without missing points in either window. Derived reports
are beside the original November 3 result; raw metrics and analyzer verification
are under `benchmarks/omb/results/2026-11-04/`. The
expanded-volume comparison remains running and no parity claim is made.

### Produce frame validation (2026-11-04 UTC)

The Produce handler now checks for trailing bytes before starting any partition
write. Previously the connection-level check ran after the handler, allowing a
malformed frame to append records before disconnecting. Six new real-client
encoder cases cover trailing/truncated multi-partition frames with acks -1, 0
and 1; all verify unchanged partition offsets. Together with existing parallel
ordering/error and partition-limit cases, eight targeted tests pass on native
PostgreSQL 17.11/default SQL 0.1.7. Gateway Clippy passes with warnings denied.
Evidence is in `tests/results/2026-11-04-produce-frame-validation/`. The disposable
cluster was removed. The active OCI comparison retains its pinned older binary;
this correctness fix has not been deployed or benchmarked there.

### Atomic committed-entry application (2026-11-04 UTC)

Opt-in SQL 0.1.12 and a typed Rust handle now apply committed journal entries to
real broker partitions while saving the applied index, membership and response in
the same transaction. Thirteen Rust integration tests passed (two new plus all
11 prior storage cases), along with six new SQL application tests and six existing
journal cases. A later purge guard passed a final seven-case SQL application run;
the opt-in crash case was skipped there and verified separately. That immediate
PostgreSQL crash preserved record bytes, append timestamp, next offset and the
applied marker, with retry returning the saved result without duplicate append.
Clippy passed with warnings denied. Evidence is in
`tests/results/2026-11-04-raft-application/`; disposable clusters were removed.

This is not the complete RaftStateMachine/snapshot interface or a running
replicated broker. Topic provisioning, replicated retention/deletion/groups,
replica transport and isolation from legacy writable entry points remain pending.
Default SQL stays 0.1.7 and live benchmark artifacts remain unchanged.

### Replicated command admission (2026-11-04 UTC)

`PartitionCommand` is now opaque and validated on construction/deserialization
using the shared Kafka record codec. Two new unit tests cover valid wire round
trips, empty/corrupt/oversized records, invalid hex, unknown fields, fractional or
negative numeric fields, timestamp bounds and offset overflow. All three cluster
unit tests pass, along with both PostgreSQL application integration cases and
all-target Clippy. Evidence is in `tests/results/2026-11-04-raft-admission/`.
The dependency change reuses already-pinned broker-protocol and hex 0.4.3; no
benchmark binary or runtime dependency was changed on OCI. Network framing and
snapshot admission still need their own bounds before deployment.

### Durable paged snapshot capture (2026-11-04 UTC)

Opt-in SQL 0.1.13 now captures a partition's retained interval into durable
snapshot tables in pages of at most 128 batches/8 MiB. Seven SQL tests passed,
including fresh/upgrade paths, append concurrency across the captured boundary,
pins against source mutation, repeatable-read serialization, count/byte bounds,
rollback, permissions, release limits and an immediate PostgreSQL crash between
pages. The crash resumed at cursor 128 of 130 and produced byte/timestamp-identical
contents. Three Rust/PostgreSQL tests passed: one typed capture/release/fencing
case and both application regressions. All-target Clippy passed.

Evidence is in `tests/results/2026-11-04-raft-snapshot/`; temporary clusters were
stopped and removed. This supplies local capture and release only. Export,
receiving/installing snapshots, OpenRaft snapshot traits and network recovery
remain unimplemented. Source pins can prevent retention while capture is
unfinished; resumable release provides an explicit abort path. Default SQL and
active OCI benchmark artifacts remain unchanged.

### Bounded completed-snapshot reads (2026-11-04 UTC)

Opt-in SQL 0.1.14 and a typed Rust reader passed seven SQL tests for versioned
installation, permission/fencing checks, stable captured bytes, count/byte
pagination, keys/null values/headers, empty images and nonzero retention bounds.
Four Rust/PostgreSQL tests passed (one new read/integrity case plus existing
application/capture tests). The reader rejects a deliberately corrupted batch.
Clippy passed with warnings denied. Evidence is in
`tests/results/2026-11-04-raft-snapshot-read/`; disposable clusters were removed.
No benchmark artifact changed. Portable snapshot framing, integrity/installation,
OpenRaft integration and network recovery remain pending.

### Produce transaction batching (2026-11-04 UTC)

The binary append path now uses one confirmed PostgreSQL transaction per Produce
request, with canonical partition lock order and duplicate-entry ordering. Eleven
targeted wire tests passed, including database-error rollback, actual transaction
ID observation, opposite-order concurrent requests, unrelated-request progress,
expanded input bounds and malformed frame rejection. The complete native suite
then passed 100 tests with four opt-in crash cases skipped (verified separately).
Gateway Clippy passed with warnings denied. Evidence and source hashes are in
`tests/results/2026-11-04-produce-batching/`.

The first full run had 99 passes and one failure because the lock-observation
test matched only the old SQL prefix. Its corrected observer verifies the exact
blocking backend and recognizes the append function within the recursive query;
the rerun passed in 89.78 seconds. The initial output is retained. Default SQL
remains 0.1.7 and the candidate uses the existing binary extension API. OCI build
and performance comparison are separate gates; local tests do not prove a gain.

### Bounded snapshot stream (2026-11-04 UTC)

The experimental Rust snapshot exporter and reader add versioned framing and
SHA-256 whole-image verification using pinned sha2 0.11.1. Seven cluster unit
tests pass, including four stream cases covering empty/multipage images, every
truncation point, checksum corruption, total/frame bounds, reordering, gaps,
manifest changes and cancellation poisoning. Four PostgreSQL integration cases
pass, including a stream round trip through a 32-byte asynchronous pipe. The
backpressure test checks a separate SQL request through a one-connection pool
after the exporter emits its frame header, proving it releases the connection
before blocking on payload IO. All-target Clippy passes with warnings denied.

Evidence: `tests/results/2026-11-04-raft-snapshot-stream/`. This adds no installation
SQL and changes no live benchmark binary. Earlier received pages must remain
staged until the final checksum verifies. Authenticated transport, durable
staging/atomic installation and OpenRaft snapshot integration remain pending.

### Durable snapshot-file staging (2026-11-04 UTC)

Eleven cluster unit tests and four PostgreSQL integration cases passed, with
all-target Clippy warnings denied. The file receiver bounds downloaded bytes,
validates the whole image, syncs it, and publishes without overwriting an existing
file. Tests include malformed/oversized input, cancellation, saved-partial recovery,
trailing bytes, reopening and a real PostgreSQL export/stage/read round trip.
Evidence: `tests/results/2026-11-04-raft-snapshot-file/`.

This is filesystem staging only. Saved-partial recovery tests are not host
power-loss tests. PostgreSQL atomic installation, authenticated replica transport,
OpenRaft snapshot integration and RF3 failure/scaling validation remain pending.
The live benchmark binary and SQL version were not changed.

### First Produce-batching matched OMB pair (2026-11-04 UTC)

The original server invocation `142e323f16bd471e9083a4c1712d5d95` exited successfully.
Full artifact/environment validation and both clean-restart checks passed. With
640 GiB/10 VPU storage and the separate unmodified Kafka client, Kafka/broker
end-to-end p99 was 188/148 ms at approximately 75,030 records/s each. Both had
zero publish errors and zero logged client warnings/errors; final backlog was
250/0. All 90 windows cover the requested 15-minute measurements. The first
trial's p99 ratio is 0.787; repetitions, capacity bounds and RF3/scaling remain
pending. Complete evidence is in
`benchmarks/omb/results/2026-11-04/producebatch75k-1/` and its comparison report.

### Second Produce-batching matched pair (2026-11-04 UTC)

Trial two's server invocation `2bf6d0a84cc94963bd730ece2507cb20` exited successfully.
Both recovery checks and complete captured-environment validation passed, including
cross-trial artifact/resource consistency and independent stores. At approximately
75,025 records/s Kafka and 75,027 broker, p99 was 206/96 ms (ratio 0.466). Both had
zero publish errors and no logged client warnings/errors; final backlog was
44/554. The combined two-trial report retains every outcome at
`benchmarks/omb/results/2026-11-04/producebatch75k-through-trial-2.json`.
Trial three's invocation `612e6753c5634eb29c8608d9fb698d8a` failed after its
broker measurement: the desktop-controlled artifact handoff timed out. Kafka and
broker restart verification did not run, so this is not a completed comparison.
The retained evidence is in `producebatch75k-3-failed/` under the same results date.

The fresh fourth attempt uses a cloud-hosted controller on spare CPUs 2–3,
allowing handoffs to continue independently of desktop suspension. The benchmark
binary, workload and server runner hashes remain frozen. Controller invocation
`54393513a0964edc85974a824bf7b857` and pair invocation
`566ba0507cfc4b91b3aab39049df5adf` were observed running; this is not a completion
claim. The controller/handoff/analysis suite passed 48 tests before the diagnostic
counter change below. Capacity and distributed acceptance gates remain pending.

Log review found one INFO-level `invalid full fetch response` diagnostic during
consumer shutdown in each of broker trials 1, 2 and the incomplete trial 3.
Zero WARN/ERROR counters therefore do not mean absence of protocol diagnostics.
The analyzer now counts invalid full/incremental fetch responses separately at
INFO/WARN/ERROR levels. Root cause and impact remain unproven; shutdown timing
alone does not establish that the response is harmless. Historical artifacts and
the running cloud analyzer are unchanged.

The updated analyzer passes all 49 harness tests. The additive audit in
`benchmarks/omb/results/2026-11-04/fetch-diagnostic-audit/` records input hashes,
line numbers and surrounding log context without rewriting historical reports.
Kafka reference logs in both completed trials have zero such events.
Inspection of Kafka 3.6.1's `AbstractFetch.maybeCloseFetchSessions` and
`FetchSessionHandler.Builder.build` suggests a possible outstanding-response
race: constructing the empty closing request replaces the full-fetch partition
set before polling pending responses. This is a hypothesis, not a reproduction
or proof that the gateway is correct.

An isolated reproduction with the unmodified Maven Kafka 3.6.1 jar now confirms
that mechanism: the same full Fetch v4 response is accepted normally and after
`notifyClose` alone, but rejected with the exact extra-partitions diagnostic
after building the empty closing request. Source, dependency hashes, Java version
and successful output are preserved in
`benchmarks/omb/results/2026-11-04/fetch-close-reproduction/`. This is client-state
evidence, not a packet-level reproduction of the benchmark or a no-loss proof.

### Exclusive partition application (2026-11-04 UTC)

The opt-in 0.1.15 migration protects bound partitions from legacy appends,
retention mutations and deletion. Six real PostgreSQL ownership tests passed
against a disposable local cluster, covering upgrade/fresh install and an older
repeatable-read writer, an in-flight append/bind race, inherited default grants,
and committed apply/retry/rollback. Evidence is in
`tests/results/2026-11-04-raft-ownership/`. The default extension version and
running cloud artifacts remain unchanged. This is a replication prerequisite,
not RF3, snapshot-installation or performance acceptance.

The existing Rust application, snapshot capture/release and snapshot stream/file
integration suites were also run against SQL 0.1.15: all four tests passed.
Evidence is in `tests/results/2026-11-04-raft-ownership-rust/`. This checks that the
new ownership guard permits the supported application and snapshot paths without
claiming consensus or network recovery.

### Third completed Produce-batching pair and storage expansion (2026-11-04 UTC)

The fourth attempted pair completed successfully under invocation
`566ba0507cfc4b91b3aab39049df5adf`; the cloud controller also exited successfully.
Both restart checks and complete environment/artifact verification passed. Kafka
and broker respectively published 75,038 and 75,049 records/s, with p99 220/127 ms
and zero publish errors. Both final backlogs were zero; broker peak backlog was
13,163 and its fitted trend was +0.858 messages/s. This peak and trend remain in
the report, rather than inferring long-run capacity from the final zero backlog.
Kafka logged two NOT_LEADER_OR_FOLLOWER-related warnings before measurement;
broker logged one invalid-full-fetch diagnostic during shutdown and 18 gateway
header deadlines. All 90 windows cover the complete requested 15 minutes.

The combined report `producebatch75k-completed-trials-1-2-4.json` under
`benchmarks/omb/results/2026-11-04/` validates three independent matched pairs and
explicitly lists the failed third attempt separately. Broker storage metrics
include all 13 conservative interior minutes with zero throttled I/O. Kafka
metrics are preserved separately with missing samples explicitly distinguished
from zero. These are fixed-load RF1 results, not capacity or distributed acceptance.

After the pair was terminal and collected, the idle-lock controller expanded the
same volume from 640 to 1024 GiB at 10 VPU/GB. Cloud state is AVAILABLE, autotuning
is disabled, and guest filesystem verification showed 459,973,840,896 bytes free.
Historical stores remain intact. Evidence: `volume1024-setup/` under the same
results date. New measurements require fresh matching references and must not be
pooled with the 640 GiB trials. The subsequent 100k launch is recorded below.

The expanded profile's OCI metrics subsequently reported 25,000 guaranteed IOPS
and throughput 468.75 at 06:12 UTC, retained in `volume1024-metrics-initial/`.
The checksum-pinned 100k bundle was then installed under idle locks on both hosts.
A fresh Kafka-first pair is running as `omb-cloud-pair-017-capacity100k-1.service`,
invocation `8ce4ee09a4aa412083a17775720f64eb`, with outer controller invocation
`e21c24d93ca14891abadffc94bbb3086`. This is a launch observation, not a completed
measurement. Default SQL and the Produce-batching gateway binary are unchanged.
Deployment hashes and captured source manifest are in `capacity100k-deployment/`
under the same results date. These results must remain separate from 640 GiB runs.

### Journal-applied retention (2026-11-04 UTC)

Opt-in SQL 0.1.16 and `PartitionCommand::retain` add explicit replicated retention
boundaries. Nine real PostgreSQL tests, twelve Rust unit tests and five Rust
PostgreSQL integration tests pass; all-target Clippy passes with warnings denied.
Evidence is in `tests/results/2026-11-04-raft-retention/` and
`tests/results/2026-11-04-raft-retention-rust/`. This validates local atomic
application and fencing, not network replication or leader-side retention policy.
The cloud benchmark remains on its frozen SQL 0.1.7 and gateway artifacts.

Two additional immediate-stop cases bring the retention PostgreSQL suite to 11
passes. A confirmed deletion and its frontier/applied marker survive recovery;
a deletion performed in an uncommitted transaction rolls back together with its
marker. After recovery, a new owner applies/replays the same committed entry and
the old owner is fenced. Evidence and PostgreSQL recovery logs are in
`tests/results/2026-11-04-raft-retention-crash/`. These are local database-process
crashes, not host-power-loss, network partition or RF3 failure tests.

### OpenRaft migration probe (2026-11-04 UTC)

The isolated `tests/experiments/openraft-compatibility/` workspace passes three
tests: nine selected JSON round trips from 0.9.25 to 0.10.0-alpha.37 and back,
plus two probes confirming the new default streaming reader still performs a
complete range read. A 4,097-entry request fails against a 4,096-entry cap before
yielding any entries. This identifies required adapter work; it is not evidence
of a successful storage migration or RF3 integration. Production pins and live
benchmark artifacts remain unchanged. Logs and source hashes are retained in
`tests/results/2026-11-04-openraft-compatibility/`.

### Completed 100k capacity probe (2026-11-04 UTC)

The original Kafka-first pair completed successfully at 06:52:09 UTC and its
environment/restart evidence passed validation. Both systems ran at least 900
measured seconds (Kafka 90 windows; broker 89 longer windows). On the new 1024 GiB,
10 VPU/GB profile, Kafka published 100,061.95 messages/s with end-to-end p99
363.001 ms; the broker published 91,378.93 messages/s with p99 606.003 ms, a
1.6694 ratio. Both reported zero publish errors. Broker scheduling delay grew
from 7.835 seconds in the first measured window to the upstream histogram's
60-second cap. Its small final consumer backlog (1,483 records) does not make
this sustainable: the producer was falling behind its offered schedule.

This load fails the performance target and is retained without rerunning or
discarding it. It does not establish maximum sustainable broker capacity.
The prior three 75k pairs remain separate because their volume profile differs.
Broker storage metrics contain all 13 conservative interior minutes with zero
throttled I/O. Kafka has 13 of 14 interior points, zero observed throttled I/O,
and a missing 06:27 point. These aggregates do not identify the bottleneck.

The broker client retained one invalid-full-fetch INFO event during shutdown,
zero WARN/ERROR events, and 19 gateway frame-header deadlines of unknown cause.
Kafka retained two premeasurement metadata-retry warnings. Both restart checks
passed; this is process restart evidence, not host-loss or RF3 acceptance.
Evidence: `benchmarks/omb/results/2026-11-04/capacity100k-1/`, the adjacent
`capacity100k-1-comparison.json`, and the two volume-metrics directories.
Next performance work should profile the overload separately and test fresh
matched intermediate loads; network Raft, snapshot installation and distributed
failure/scaling acceptance remain outstanding.

### Active overload diagnosis (2026-11-04 UTC)

A separately marked broker-only 100k diagnosis was launched with the unchanged
Produce-batching gateway and SQL 0.1.7, on the 1024 GiB profile. The deployed
harness passed 55 tests; the subsequent local analyzer brings that to 58.
Outer invocation `f4fa5ca901574cf88f398e5eae4e13ae` launched pair invocation
`10e5c9314f1b4ebe857e893cefcfefa2`. Both sampling and the ordinary client handoff
were observed live. Deployment source hashes are in
`benchmarks/omb/results/2026-11-04/diagnostic100k-deployment/`.

The interim 07:01–07:06 UTC window contains 150 samples with no errors and
298.02 seconds covered by valid intervals. PostgreSQL averaged 0.775 cores and
the gateway 0.342, with zero recorded cgroup CPU throttling. Active backend
observations include 51 WalSync, 10 WALWrite-lock and 102 with no reported wait.
These counts are not elapsed wait time, and no reported wait does not prove CPU
execution. Evidence in `diagnostic100k-1-interim/` includes the client measurement
start, source analyzer and tests. The full run and restart check remain pending;
this is diagnostic evidence, not performance acceptance or a proven bottleneck.

The same diagnostic invocation subsequently exited successfully and was collected.
It measured 900.834 seconds (89 windows), published 89,372.47 messages/s, reported
zero publish errors and end-to-end p99 618.003 ms, and again reached the upstream
60-second scheduling-delay cap. All 498 lifetime samples were valid; the normal
post-workload sampler stop terminated pidstat with signal 15. The restart/storage
verification passed. This instrumented result is not pooled with the uninstrumented
100k pair.

Full measurement-window analysis selects 450 samples covering 898.065 seconds of
valid intervals: database CPU averaged 0.803 cores and gateway CPU 0.347, with zero
cgroup CPU throttling. Active backend observations were 132 WalSync, 36 WALWrite
lock waits, 334 with no reported wait, 12 DataFileExtend, one WalWrite and four
ClientRead. The whole diagnostic lifetime (994.963 seconds, including warmup)
recorded 287,547 WAL syncs and 1,839,490,250 WAL bytes. WAL timing was disabled,
so mean write/sync latency is explicitly unavailable. The 847,858 database commits
also include read-only and sampler transactions and are not Produce counts.

Evidence: `diagnostic100k-1/`, `diagnostic100k-1-comparison.json` (explicitly a
single-system diagnostic report), and `diagnostic100k-1-analysis.json` under
`benchmarks/omb/results/2026-11-04/`. The analyzer now checks statistics resets and
disabled timing; all 61 harness tests pass, with source/test evidence in
`tests/results/2026-11-04-diagnostic-analysis/`. These observations motivate a
separate WAL/function-timing diagnostic; they do not establish whether validation,
SQL execution, synchronization or network round trips dominate the produce path.

### Active WAL/function timing diagnosis (2026-11-04 UTC)

After the preceding job was terminal and the host idle, a fresh broker-only 100k
diagnostic launched with the same gateway, SQL and resource profile. The deployed
harness passed 63 tests; local function-delta analysis adds two more passes.
Outer invocation `5038ae8e573c4bdcb57dbfbf71d39250` launched pair invocation
`147112804c714f5eaa800b0b46ce4074`. The latter was observed running at 07:21:04 UTC.
The captured settings are `on|on|all` for I/O timing, WAL timing and function
tracking. The pre-workload clock test measured 34.82 ns per loop including its
overhead; this is not a measurement of overall profiling overhead. Sampling was
observed live. Deployment hashes are in
`benchmarks/omb/results/2026-11-04/timing100k-deployment/`.

The workload and restart check remain pending. This diagnostic cannot establish
performance acceptance; its purpose is to distinguish WAL synchronization from
extension/SQL execution cost without weakening acknowledgment durability.

An interim read-only snapshot over 185.294 seconds of diagnostic lifetime recorded
50,507 WAL syncs, 58,912.304 ms aggregate sync time (1.1664 ms per sync), and
1,475.025 ms aggregate WAL write time. These totals include startup/warmup and
are not a wall-clock stall fraction. Tracked append entry points had 59,332 calls
and 30,906.75 ms inclusive time; `broker_call` had 83,663 calls and 22,597.268 ms
inclusive time. Nested SQL-helper times must not be added to their callers.
The ordinary OMB consumer commits offsets asynchronously after nonempty polls;
the workload has meaningful offset-commit work as well as produces. Evidence is
in `benchmarks/omb/results/2026-11-04/timing100k-1-interim/`.

Host counter analysis now handles gaps and decreasing counters explicitly and
does not add guest time to the first eight CPU fields. A 500 µs commit-delay /
one-sibling experiment has been prepared locally, restricted to timed broker-only
diagnostics, with 69 harness tests passing. Source hashes and test evidence are in
`tests/results/2026-11-04-group-commit-preparation/`. It has not been deployed or
performance-verified; the current baseline continues with its original artifacts.

A one-shot observer now waits for both original timing-baseline invocations to
exit successfully and for the collector to validate and publish the completed
evidence. Before deploying the frozen group-commit candidate, it checks the full
measurement, restart/storage result, zero sampling errors, enabled timing,
observed baseline settings `0|5|on|on|on`, unchanged prepared source hashes, and
that overload plus at least 0.5 ms mean WAL sync cost persist in the final data.
Deployment additionally requires the idle-host lock and existing resource
preflights. Observation expiry does not stop or replay either workload. Source
and queued state are retained in
`benchmarks/omb/results/2026-11-04/groupcommit500-queue/`.
At 07:30:54 UTC both baseline services were still live under their original
invocation IDs. The candidate had not launched.

The timing baseline later completed and was collected successfully: 900.974
measured seconds, 90,495.26 messages/s, zero publish errors, end-to-end p99
619.003 ms, scheduling delay at the upstream 60-second cap, and 497 valid lifetime
samples. Restart/storage verification passed. Full analysis is retained in
`benchmarks/omb/results/2026-11-04/timing100k-1-analysis.json`. It records 291,526
WAL syncs over 993.005 lifetime seconds, averaging 0.97167 ms. Database/gateway CPU
averaged 0.803/0.350 cores during measurement. Host CPU0/CPU1 idle counter fractions
were 34.2%/35.4%; these are host-wide counters, not container attribution.

After both baseline services exited, collection and all guard checks passed,
the frozen candidate launched once. Outer invocation
`c4eb23a7f7404595a2f0f1b7ca0fabe6` launched pair invocation
`3894dbb0d3974862b46d3eb3ebcda9f8`, observed live at 07:39:03 UTC. Effective settings
are `commit_delay=500`, `commit_siblings=1`, with fsync, full-page writes and
synchronous commit still on. The binary and SQL remain unchanged. Deployment
hashes are in `groupcommit500-deployment/` under the same results date.

The diagnostic comparison tool now checks the two reviewed runner/sampler
revisions and their shared client/binary/SQL/resource contract, preserving errors,
scheduling-delay censoring and tails. All 72 harness tests pass; test/source
evidence is in `tests/results/2026-11-04-group-commit-comparison/`. The real completed
baseline also passes its inspection. Candidate measurement and restart results
remain pending; no group-commit performance improvement is claimed yet.

Local bulk offset-commit candidate (2026-11-04): opt-in SQL 0.1.17 and gateway
capability negotiation now pass 53 PostgreSQL/Kafka-client integration cases,
plus a seven-case focused run (five overlap, 55 unique cases in total). The focused
run covers fresh installation and upgrade, duplicate ordering, partial domain
errors, whole-request SQL rollback, malformed input, private ACLs, and an
instrumented wire request proving one expiration check on 0.1.17 versus three
on the 0.1.7 fallback. Existing wire tests verify ownership, stale generations,
departed members and failed commit rollback. Both disposable clusters kept fsync,
full-page writes and synchronous commit enabled. Gateway build and Clippy with
warnings denied pass. Evidence and source hashes are retained in
`tests/results/2026-11-04-bulk-commit/`. Group validation occurs once while holding
the durable group lock until transaction completion; it is not re-evaluated at
each partition's wall-clock time. Default installation and the running cloud
benchmark remain on SQL 0.1.7; no performance improvement is claimed for 0.1.17.

The commit-delay diagnostic subsequently completed: 901.593 measured seconds,
91,290.60 messages/s, zero publish errors, end-to-end p99 612.003 ms, and successful
restart/storage verification. Relative to the instrumented baseline, throughput
was 1.00879 times and p99 0.98869 times; both runs reached the upstream scheduling
delay cap. WAL sync count fell from 291,526 to 266,223 across their respective
whole sampling lifetimes. This single run does not establish a repeatable gain
or sustainable 100k capacity. The default commit delay remains zero. The complete
comparison is `benchmarks/omb/results/2026-11-04/groupcommit500-1-versus-timing100k-1.json`.

The bulk-commit gateway was then built offline on the idle OCI ARM64 host:
SHA-256 `81ceb16a9475cc5e35cd7f34e67c9132e8efe7418a4e3e9e3489f4abf4802103`.
The harness now supports a checksummed SQL 0.1.17 candidate only in timed
broker-only diagnostic mode, with commit delay zero. All 74 harness tests pass.
It requires observed calls to the private bulk helper before accepting a run.
Frozen sources are in `tests/results/2026-11-04-bulk-commit-preparation/`.
This candidate also includes intervening SQL upgrades, including ownership checks;
its result measures the complete candidate, not an isolated causal estimate of
the bulk function alone. Outer service invocation
`0194d20942e1428ab44582bb3c2efd3d` launched the next diagnostic; results are pending.

That first bulk-candidate attempt failed before measurement: SQL uploads had mode
0600, and `docker cp` preserved it, preventing the PostgreSQL UID from reading the
migration. Pair invocation `d03625d99c39414e98e0bed3f08639b6` is terminal failed;
its evidence is preserved under `bulkcommit-1-failed/`. The runner now explicitly
sets copied migration files to 0644, and deployment also uses readable SQL modes.
A real shell regression test starts with a 0600 source and restrictive umask and
verifies the output permissions. All 75 harness tests pass. The corrected run uses
the new label `017-bulkcommit-2`; the failed invocation was not replayed.
Outer invocation `a3bcfa7640c54515a345255658c8f858` and pair invocation
`470977121aa440c7932e1abae7c69bf0` were confirmed live at 08:04:26 UTC.
The SQL upgrade completed, and effective commit settings are `0|5`. The observer
collects this exact invocation only and requires full-duration measurement,
restart/storage validation, zero sampling errors and positive bulk-helper calls.
The new run's performance result remains pending.

At 08:06:58 UTC, a read-only function-counter observation confirmed the new path:
34,458 `broker.commit_many` calls, 34,537 expiration-helper calls and 208 generic
dispatcher entry calls. These are lifetime counters including startup/warmup, not
measurement throughput. The comparison now checks the frozen candidate binary,
all ten intervening migration hashes, runner/sampler revisions, unchanged workload
and resource contract, durable restart evidence, default commit-delay settings,
full measurement duration and positive bulk-helper call deltas. It keeps censored
scheduling delay and client errors in the report. All 79 harness tests pass, and
the real completed baseline still passes the 20-field inspection. Source and test
evidence is in `tests/results/2026-11-04-bulk-commit-comparison/`. A local observer
will write the comparison after the original run's collector completes; it cannot
stop or replay any workload. Final candidate throughput remains pending.

The running candidate subsequently showed provisional 79–81k message/s windows
with growing producer delay. Its read-only interim function snapshot is retained
in `benchmarks/omb/results/2026-11-04/bulkcommit-2-interim/`. It records 118,539
appends and 237,078 ownership-helper calls: two helper calls per append. Nested
times are not additive. This is an incomplete run and does not establish the
cause of the observed regression.

Opt-in SQL 0.1.18 therefore removes the redundant lock acquisition from the
partition UPDATE trigger while retaining the binding/context check and explicit
batch-write lock. PostgreSQL's [row locking documentation](https://www.postgresql.org/docs/17/explicit-locking.html)
and [trigger implementation](https://raw.githubusercontent.com/postgres/postgres/REL_17_STABLE/src/backend/commands/trigger.c)
were checked: local-row UPDATE obtains its tuple lock before calling the BEFORE
row trigger. Fifteen native PostgreSQL cases pass, covering upgrade/fresh install,
private privileges, rollback, committed application, blocked binding and stale
snapshots. An added direct UPDATE race waits on an uncommitted binding and then
fails with 55000 at READ COMMITTED or 40001 at REPEATABLE READ. Instrumentation
proves one explicit ownership-helper call per append versus two in SQL 0.1.17.
Evidence is in `tests/results/2026-11-04-raft-guard/`. This is not deployed to the
running cloud benchmark and has no claimed throughput improvement.
The follow-up Kafka-client/wire/broker/bulk suite also passed all 55 cases against
a disposable SQL 0.1.18 main database (isolated compatibility fixtures retain their
specified versions). Fsync, full-page writes and synchronous commit stayed enabled.
Its logs, settings and runner are preserved alongside the 15 ownership cases.

The complete SQL 0.1.17 bulk candidate was later collected from its original
successful invocation after retrying a transient SSH collection failure. It ran
900.615 measured seconds, averaged 85,260.79 messages/s, reported zero publish
errors and end-to-end p99 644.003 ms, and again reached the scheduling-delay cap.
All 496 sampler observations were valid; restart/storage checks passed. Relative
to the earlier timed SQL 0.1.7 baseline, throughput was 0.94216 times and p99
1.04039 times. This is a measured regression of the complete candidate. The
comparison is `benchmarks/omb/results/2026-11-04/bulkcommit-2-versus-timing100k-1.json`.
Its lifetime counters show 323,119 append calls and 646,238 explicit ownership
checks. Mean inclusive append time was 0.66738 ms versus 0.53602 ms in the earlier
baseline; nested timings must not be summed. This does not isolate a causal effect.

The frozen SQL 0.1.18 diagnostic uses the same gateway binary, workload and
durability settings and is queued only after both preceding services terminate
successfully, collection validates, and the full comparison exists. Sources are
in `tests/results/2026-11-04-partition-guard-preparation/`; the one-shot observer is
preserved in `benchmarks/omb/results/2026-11-04/partitionguard-queue/`. The SQL-only
comparison checks reviewed migrations, binary/resource equality and positive
ownership-helper calls, without accepting repeatability or Kafka parity. All
84 harness tests pass; comparison evidence is in
`tests/results/2026-11-04-partition-guard-comparison/`.
After the prior run's original invocations and complete comparison passed the
guards, the SQL-only candidate launched once. Outer invocation is
`195eef4f26af48429f4e262f4fb18066`; pair invocation is
`23f3e1280d7c4e3a98bae883c2836607`. Both were observed live. The collector tracks
that exact pair and compares it against completed `bulkcommit-2` only after
measurement and recovery validation. Deployment hashes are preserved in
`benchmarks/omb/results/2026-11-04/partitionguard-deployment/`. Results are pending.

Measurement-window analysis of completed `bulkcommit-2` is retained in
`benchmarks/omb/results/2026-11-04/bulkcommit-2-analysis.json`, using the UTC client
start at 08:04:46.653 and aggregate boundary at 08:19:47.456. Across 450 samples,
database/gateway usage averaged 0.7768/0.3396 CPU cores, compared with
0.8028/0.3504 for the earlier baseline. Lower total CPU accompanies lower
throughput and does not by itself establish greater efficiency. Active-backend
WAL-sync observations were 123 versus 124; these are observation counts, not wait
durations. Whole-lifetime mean WAL-sync time was 1.01315 ms versus 0.97167 ms.
Host CPU0/CPU1 idle fractions were 36.0%/37.3%; the measurements do not demonstrate
CPU saturation. The SQL-only experiment remains necessary to test the proposed
query-cost reduction; no throughput improvement is inferred from these counters.

The live SQL 0.1.18 run was checked separately: 92,762 append entry calls and
92,762 explicit ownership-helper calls confirmed the expected one-to-one count.
It remained on its original invocation and frozen binary.

A subsequent local gateway candidate moves the primary-status check from pool
recycling into the already pipelined transaction-settings exchange, using
`pg_catalog.pg_is_in_recovery()`. New connections still require read-write target
session attributes. Failed settings, checks, queries or commit confirmation still
discard the physical connection; successful responses still require confirmed
COMMIT. A stale socket may now fail its first request rather than being rejected
during pool checkout; normal Kafka retry semantics apply. No automatic replay of
an ambiguous write is added.

All 51 native cases pass on SQL 0.1.18, including injected primary-check failure
before a pipelined Produce, zero record changes after failure, new physical
connection on retry, network blackholes/readiness cancellation, database crash
recovery and missing synchronous standby acknowledgment refusal. Nine focused
cases also pass on default SQL 0.1.7. Gateway build and Clippy pass. The check
failure test injects a false predicate through a local PostgreSQL wire proxy; it
does not claim a physical standby failover test. Evidence and frozen source hashes
are in `tests/results/2026-11-04-primary-pipeline/`. The offline ARM64 build is
queued behind completed collection of the live SQL-only run. This candidate is
not deployed and has no measured throughput benefit yet.

Read-only result collection now has a separate bounded-retry helper. It retries
SSH transport exit 255 and transport timeouts only, never ordinary remote command
failures. It verifies the same service invocation before and after downloading a
successful completed run. Downloads use exclusive new files and truncate partial
bytes before retrying, preventing concatenated archives or overwritten evidence.
It exposes no workload launch/restart operation. Four tests cover invocation
replacement, nontransport failures, partial-stream retry, overwrite refusal and
active/failed-run rejection; all 88 harness tests pass. Source and logs are in
`tests/results/2026-11-04-read-only-collection/`. A replacement collector using the
helper is prepared for a future retry; the currently running observer is unchanged.

The SQL 0.1.18 diagnostic completed and validated: 900.390 measured seconds,
88,227.68 messages/s, zero publish errors, end-to-end p99 626.003 ms and successful
restart/storage checks. Throughput was 1.03480 times SQL 0.1.17 and p99 0.97205
times; ownership-helper calls per append fell from exactly two to one. Both runs
still reached the scheduling-delay cap and retained one invalid-fetch-response
INFO event in the full client log, with zero warning/error/offset-commit-failure
events. A single instrumented run does not establish repeatability or sustainable
100k capacity. The complete comparison and CPU/I/O analysis are
`partitionguard-1-versus-bulkcommit-2.json` and `partitionguard-1-analysis.json`
under `benchmarks/omb/results/2026-11-04/`.

The queued primary-check gateway build then completed offline with Rust 1.92.0,
binary SHA-256 `80e1eaa144e5ddf859b9322396b8ba89f2cfb82b4b26ec71b7ee584ce77068be`.
Its source archive SHA-256 is
`fd85749febf03bded5206bb24425de7eb207600d35cce3dbe8bee1bac261e555`.
The next comparison requires identical SQL 0.1.18, harness, workload and resource
evidence, allowing only the explicitly supplied reviewed gateway binary digest
to differ. All 91 harness tests pass; comparison source/tests are preserved in
`tests/results/2026-11-04-primary-pipeline-comparison/`. A new fresh-store diagnostic
uses label `018-primarypipeline-1`; its performance remains unverified.
Its outer invocation `2592713f9d8e4c1d855f0dd0ef9890c6` and pair invocation
`b814f5e0c87f47128e560f3fb20a3a2a` are live. Effective commit settings remain
`0|5`; SQL installation passed. The collector uses bounded read-only retries and
checks the exact invocation before and after transfer. No successful performance
result is claimed for this running candidate.

A local transport candidate now constructs one bounded Kafka response buffer
containing length, correlation ID and body, replacing the deliberate separate
prefix and payload writes. This preserves the 8 MiB payload limit including the
correlation ID and the ten-second write deadline. It does not promise one TCP
packet or one syscall: partial writes remain possible. Three protocol tests pass,
including exact bytes and maximum/oversized boundaries, as do 51 native client,
wire, network and durability cases. Build and Clippy pass. Evidence and frozen
source hashes are in `tests/results/2026-11-04-response-frame/`; an offline build
is queued after completed collection of the current primary-check diagnostic.
The live binary is unchanged and no transport throughput benefit is claimed.


The primary-check pipeline diagnostic subsequently completed and was collected
against its original invocation: 900.703 measured seconds, 91,782.98 messages/s,
zero publish errors, end-to-end p99 605.003 ms and successful restart/storage
checks. Relative to the preceding SQL 0.1.18 run, throughput was 1.04030 times
and p99 was 0.96645 times. Both reached the producer scheduling-delay histogram
cap; this is not sustainable 100k capacity or a repeatability result. The full
client log retained one invalid-fetch-response INFO event and zero warning,
error or offset-commit-failure events. All 497 server samples succeeded.
Evidence is in `benchmarks/omb/results/2026-11-04/primarypipeline-1/` and
`primarypipeline-1-versus-partitionguard-1.json` alongside it.

The response-framing candidate then built offline with Rust 1.92.0. Its source
archive SHA-256 is `50a9da977f790a235fbd17b988621a6fb39cd330f391e608e123f461b2381c40`
and binary SHA-256 is `530a87612ad3fd55356a9fa36dd1445c23b7abe4b9abe3d73f24133cdfc1018a`.
The next diagnostic compares it with the primary-check pipeline binary on the
same SQL 0.1.18 and frozen harness. The comparator now accepts an explicit
reviewed baseline digest and rejects equal or malformed digests; all 92 harness
tests pass. This candidate has no measured cloud performance benefit yet.

The fresh-store response-framing diagnostic was launched as
`018-responseframe-1`, outer invocation `5d30a0010da94a19b81137ace815c9c9`
and pair invocation `346e4e4010c4414e9feb93145de284ce`. Deployment evidence is in
`benchmarks/omb/results/2026-11-04/responseframe-deployment/`. Its collector
compares against the preceding primary-check pipeline candidate, requires the
same frozen SQL/harness/resource configuration, and preserves the original run.
Results are pending; the run is instrumented diagnosis, not acceptance.


A subsequent local candidate pipelines the prepared query with the transaction's
`statement_timeout=0` reset. Preparation completes before this inner pipeline,
including cold plans. Biased polling enqueues the bound query first, then the
reset; both responses must succeed before a separate confirmed COMMIT. The query
still sees the three-second statement timeout. Commit retains its five-second
gateway deadline and synchronous-commit durability setting. The unconfirmed
connection guard still discards physical connections on failure or cancellation.
This relies on pinned tokio-postgres 0.7.15 query enqueue order, inspected in
`query.rs::start`, and its documented sequential server execution of pipelined
requests: https://docs.rs/tokio-postgres/0.7.15/tokio_postgres/#pipelining.

Build and Clippy pass. The 51-case native regression suite passes; three focused
cases cover primary-check failure, reset failure after a write, and cold/cached
query settings (52 distinct native cases overall). Ten focused cases also pass
on default SQL 0.1.7. A reset failure rolls back the append, leaves the offset
unchanged and requires a new physical connection. Missing synchronous standby,
network blackhole/cancellation and database crash recovery remain covered by the
full suite. Evidence and source are in `tests/results/2026-11-04-query-reset/`.
This candidate is local only; no throughput benefit has been measured. The live
response-framing diagnostic remains unchanged.

The query/reset source is frozen in archive SHA-256
`7f4b9427a50ae64d4eefafd4f13783d62100475cbb8e989e70e4c0f00308369e`.
Its offline ARM64 build is queued behind successful completion of outer invocation
`5d30a0010da94a19b81137ace815c9c9` and verified local collection. It additionally
requires the global benchmark lock and no running containers or Java process;
it cannot build concurrently with measurement. No cloud binary or performance
result is claimed until that build and a subsequent run complete.


The response-framing diagnostic completed with 900.765 measured seconds,
89,124.29 messages/s, zero publish errors and end-to-end p99 622.003 ms.
Relative to the primary-check pipeline run, throughput was 0.97103 times and
p99 was 1.02810 times. Thus this run supplies no framing performance win. The
producer scheduling-delay histogram again reached its cap. Restart/storage
checks passed with SQL 0.1.18 and fsync/full-page-writes/synchronous-commit on.
Full-log events were zero warnings/errors/offset-commit failures and one
invalid-fetch-response INFO event. Evidence is in
`benchmarks/omb/results/2026-11-04/responseframe-1/` with comparison
`responseframe-1-versus-primarypipeline-1.json`. One diagnostic run does not
establish a causal regression or repeatable gain; 100k remains unsustainable.
The following query/reset candidate includes the framing change, so its next
isolated comparison must use this framing run as its immediate baseline, while
also retaining the best earlier result for context.


The query/reset ARM64 build completed offline under invocation
`4af447cb59e84da384fbd1b7078adadc` with Rust 1.92.0 and binary SHA-256
`9130352c6bdd0a78321ce4bbea5ea4badb9714f23222f2abfab86b433c6bf43e`.
The next fresh-store diagnostic uses the previously frozen SQL 0.1.18 harness,
not the newly prepared matched-pair tooling. Its immediate comparison baseline
is the response-framing binary
`530a87612ad3fd55356a9fa36dd1445c23b7abe4b9abe3d73f24133cdfc1018a`.
No change to the acknowledgment durability settings or client workload is made.

The query/reset diagnostic is live as `018-queryreset-1`, outer invocation
`6bf6b0b53a1d41438f688095e4c8fc94`, pair invocation
`2c48b1ae7c39419497889cd0a5960234`. Deployment evidence is in
`benchmarks/omb/results/2026-11-04/queryreset-deployment/`. Its collector is bound
to this exact invocation and uses bounded read-only transport retries. Results
remain pending; no query/reset throughput gain is claimed.


The query/reset diagnostic completed and was collected from its original
invocation: 900.792 measured seconds, 92,682.93 messages/s, zero publish errors,
end-to-end p99 601.003 ms, 496 successful server samples and passed restart/storage
checks. Relative to response framing, throughput was 1.03993 times and p99 was
0.96624 times. The result is only about 1% above the earlier primary-check
pipeline run, so it does not establish a repeatable gain over that revision.
Producer scheduling delay again reached its histogram cap; 100k remains
unsustainable. The full client log retains one invalid-fetch-response INFO event
and zero warnings/errors/offset-commit failures. Evidence is in
`benchmarks/omb/results/2026-11-04/queryreset-1/` and
`queryreset-1-versus-responseframe-1.json`. The next performance step is a fresh
matched Kafka/broker 85k pair with the reviewed query/reset binary, followed by
repeatability and capacity testing; no RF3/scaling acceptance is inferred.


The first matched 85k pair is live with the reviewed query/reset binary and SQL
0.1.18, Kafka first, fresh stores and diagnostic timing disabled. Its label is
`018-capacity85k-1`; outer invocation `aa90f53ae1334f8c852468a0803131e2`, pair
invocation `e3e4046642094bc19b7be1ccfe9f093e`. Both idle hosts received the frozen
99-test matched-pair harness. The server had 309 GiB free, exceeding the 125 GiB
preflight requirement. Deployment and installation hashes are preserved in
`benchmarks/omb/results/2026-11-04/capacity85k-deployment/`. The collector checks
both full fifteen-minute measurements, exact workload and binary/SQL contracts,
restart evidence, and absence of diagnostic instrumentation. No 85k result or
capacity/parity acceptance is claimed while this first pair runs.


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
