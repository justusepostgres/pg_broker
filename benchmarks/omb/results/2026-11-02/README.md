# OpenMessaging measurements — 2026-11-02

These are initial single-host, RF=1 measurements, not Kafka performance parity
or distributed-storage acceptance. Each fixed-rate result is one run; repeated
trials, a capacity sweep, separate client hosts and RF=3 are still required.
All raw JSON files are preserved, including the misleading automatic-rate run.

## Fixed offered load: 10,000 messages/s

One topic, 16 partitions, 1 KiB values from the upstream payload file, one producer,
one subscribed consumer, one minute warmup and five minutes measurement.
Kafka Java client 3.6.1 is used through the unmodified upstream OMB driver.
All runs use `acks=all`, no idempotence/compression, 5 ms linger, 256 KiB batches
and one in-flight producer request. See the checked-in driver/workload YAML.

| System / variant | Mean published msg/s | Publish p99 ms | End-to-end p99 ms | Maximum backlog | Final backlog |
|---|---:|---:|---:|---:|---:|
| Apache Kafka 3.6.1 | 10,007 | 9.205 | 10.000 | 46 | 0 |
| Broker original release / extension 0.1.1 | 10,007 | 52.095 | 563.003 | 6,381 | 1,079 |
| Broker batched fetch / extension 0.1.1 | 10,007 | 43.232 | 216.000 | 1,437 | 640 |
| Broker optimized / extension 0.1.2 | 10,007 | 42.448 | 60.000 | 268 | 230 |
| Broker SQL bulk fetch / extension 0.1.3 | 10,006 | 33.563 | 44.000 | 215 | 116 |
| Broker parallel produce / extension 0.1.3 | 10,006 | 27.765 | 39.000 | 198 | 85 |
| Broker binary fetch / extension 0.1.4 | 10,006 | 24.505 | 30.000 | 189 | 117 |
| Broker binary read/write / extension 0.1.5 | 10,008 | 10.595 | 16.000 | 108 | 51 |

Every fixed-rate run recorded zero publish errors. The SQL bulk-fetch variant's
0.1.3 p99 end-to-end latency was about 92% lower than the original broker, but
remained 4.4 times Kafka's (44 ms versus 10 ms). This is a single trial per variant. Small outstanding backlogs remained when the broker runs
ended. These counts do not establish a fully drained run or exactly-once delivery.
The working latency target of at most 1.25 times Kafka is **not met**. Maximum
sustainable throughput of the current 0.1.3 implementation is not established.

The optimized variant combines a single fetch transaction, indexed seeks to the
batch covering a requested offset, commit-time notification wakeups with polling
fallback, and removal of a redundant payload copy from the pgrx SPI request.
The combined run cannot attribute improvements to individual changes.

## Environment and durability

- One isolated OCI VM.Standard.A1.Flex: four 3.0 GHz Ampere Altra CPUs, 24 GiB RAM,
  80 GiB boot volume at 10 VPUs/GiB. No other application workloads ran during
  fixed-rate measurements; builds happened between measurements.
- Server CPUs 0–1; OMB worker CPUs 2–3, 1 GiB JVM heap. Server total memory budget
  9 GiB: PostgreSQL 8 GiB plus gateway 1 GiB, or Kafka's 9 GiB cgroup limit with
  1 GiB JVM heap. Client and server still share the host and storage subsystem.
- Fresh stores for each fixed-rate run. Kafka uses a new KRaft store; broker runs
  use fresh PostgreSQL Docker volumes. Loopback plaintext transport on this
  isolated host. This does not measure network replication or TLS overhead.
- PostgreSQL 17.5, pgrx 0.16.1, Rust 1.92.0 release builds. PostgreSQL `fsync`,
  `full_page_writes` and `synchronous_commit` remained on. No standbys.
- Kafka 3.6.1 with `log.flush.interval.messages=1`, an explicit local-flush policy
  rather than Kafka's default. Both systems use RF=1 here. This is not a result
  for Kafka's default RF=3 configuration.
- OMB revision `5b1fa70951a323da26bd587174b58bb2c65b0b5c`; OpenJDK 17.0.20.1.
  Producer/consumer source and wire protocol are unmodified.

The before and batched-fetch runs use extension 0.1.1. The optimized 0.1.2 run
installs the 0.1.1→0.1.2 SQL upgrade and mounts the rebuilt gateway and extension
library. The SQL bulk-fetch run additionally installs 0.1.2→0.1.3 and mounts its
updated gateway, retaining the same tested extension shared library.
Artifact hashes and source hashes are recorded in `artifacts.txt`; the build
recipe remains the repository Dockerfile and pinned Cargo.lock. The 0.1.2 source
archive SHA-256 is
`c33e3acec1c43e51b5fc2118c9777c6bac78afb7d8dc092d1b8b843940316ddf`.

## Automatic-rate and smoke diagnostics

The original broker's automatic-rate run averaged 11,490 published messages/s.
The batched-fetch run averaged 11,223, but the upstream controller entered an
absorbing zero-rate state near the end; its worker clamps this to approximately
one message/s. This was reproduced against the compiled upstream RateController
class (zero rate remains zero even after backlog clears). Do not interpret this
average as the optimized broker's capacity, or substitute its early peaks.

The Kafka automatic-rate diagnostic averaged 40,786 messages/s. It did not use
the later matched memory cgroup and included a brief controller-probe compilation;
it is a diagnostic reference, not a controlled acceptance result. Smoke files
verify client interoperability at approximately 100 messages/s. The Kafka smoke
run overlapped compilation and must not be used for latency comparison.

OMB also leaves an executor alive after writing results. The runner now waits
10 seconds after a complete JSON result and then cleans up the process group,
recording this separately. Initial completed JVMs were inspected and terminated
manually. No measurements were shortened to improve a result.

## Verification and next gates

The SQL bulk-fetch source passed 33 integration tests, including concurrent production,
ordering, independent groups, fencing, database/gateway recovery, backup restore,
retention, shared fetch budgets, SQL upgrades and notification recovery. Workspace
Clippy with warnings denied, formatting and both CNPG Kustomize renders passed.
The earlier multi-host CNPG tests used 0.1.1; no new 0.1.3 CNPG acceptance is claimed.

Next: reduce record-byte serialization costs, establish fixed-
rate capacity with repetitions, then validate independent PostgreSQL partition
replicas, RF=3 failure safety and horizontal scaling. The OpenRaft crate remains a
scaffold. Neither distributed partition replication nor exactly-once processing
is implemented.

The isolated OCI VM was stopped after the 0.1.2 evidence collection and restarted
for 0.1.3 SQL measurements, then verified STOPPED through the OCI API. It was
subsequently restarted for the parallel-produce follow-up; both runs and post-run
diagnostics are complete. The broker containers stopped cleanly, and the VM was
verified STOPPED again through the OCI API. Its 80 GiB
boot volume and dedicated network are retained for continued benchmarking; the
shared OKE environment was not modified. The local test PostgreSQL server is also
stopped.


## SQL bulk-fetch follow-up

Extension 0.1.3 adds a permissioned SQL-only bulk read path, with bounded requests
and the same REPEATABLE READ snapshot and byte budget. Writes still validate
record batches in pgrx. The gateway and SQL hashes are in
`sql013-source-hashes.json`; the pgrx shared library is unchanged from 0.1.2.

The 10k run contains all 30 expected measurement samples, zero publish errors
and no warning/error/exception entries in the client log. A runner script was
updated while its shell was waiting for the JVM; after the complete result and
normal post-result JVM cleanup, that shell failed parsing its remaining lines.
The summary and clean database/gateway stop were therefore performed explicitly.
This did not shorten the measurement or alter the client. Later runs use the
updated script from launch without editing it during execution.

The 0.1.3 source snapshot archive is `.dev/omb-sql013-source.tar.gz`, SHA-256
`09ac34012fb662f9e543d382074f8495e52f2854e71f4f0951141f556844c422`.
Deployed gateway, extension source, and SQL upgrade hashes match the local source.
`sql013-deployed-evidence.txt` also records SQL version 0.1.3, the active read
function permission, and enabled fsync/full-page-write/synchronous-commit settings.


## Fixed offered load: 20,000 messages/s (0.1.3)

This fresh-volume overload test used the same broker binaries, client, CPU and
memory limits, and durability policy as the 0.1.3 10k run. It completed all 30
measurement samples, with no warning/error/exception entries in the client log.

| Metric | Observed |
|---|---:|
| Offered rate | 20,000 msg/s |
| Mean published rate | 13,995.495 msg/s |
| Mean consumed rate | 13,994.668 msg/s |
| Publish errors | 0 |
| Publish p99 | 3,572.031 ms |
| End-to-end p99 | 3,555.007 ms |
| Reported producer-delay p99 | 60,000,255 µs |
| Maximum / final consumer backlog | 253 / 0 |

**The 20k offered rate is not sustained.** Throughput is only about 70% of the
offered load, and producer delay grows to the reported 60-second range. A small
consumer backlog and zero publish errors do not make this an acceptable result.
There is no matched Kafka 20k run or repeated trial yet; this is an overload
diagnostic, not a maximum-capacity estimate or a Kafka throughput comparison.
The 32k workload is available but was not run after this failed lower-rate gate.

One lightweight Docker/database diagnostic during the run sampled PostgreSQL at
111% CPU and the gateway at 36% CPU (100% denotes one core), with roughly 87 MiB
and 24 MiB reported container memory respectively. This is a point sample, not
a time series or a bottleneck proof. It and a durability/hash check ran during
the overload test; no compilation ran on the VM. The produce handler currently
commits partitions serially and the write boundary still converts record bytes
through JSON/SPI. These are candidates for measured follow-up optimization.


## Bounded parallel-produce follow-up

The gateway now commits up to four independent partitions concurrently within a
produce request. Repeated topic/partition entries stay sequential, and all
results must be confirmed before a response. The pool remains bounded at 16.
The extension library, SQL version 0.1.3, read path, and durability settings are
unchanged from the SQL bulk-fetch run. Local validation passed 35 integration
tests, Rust tests, Clippy with warnings denied, formatting and whitespace checks.
Source hashes are in `parallel4-source-hashes.json`.

The 10k run completed all 30 samples with zero publish errors. End-to-end p99 was
39 ms, versus 44 ms for serial produce and 10 ms for Kafka. This single-trial
improvement is modest and remains outside the latency target. The 20k result below confirms higher producer throughput, but poor tail latency
remains. All deployed gateway source hashes match the tested local source.


### Parallel-produce 20k result

| Metric | Serial produce | Four concurrent partitions |
|---|---:|---:|
| Mean published msg/s | 13,995.495 | 20,014.233 |
| Mean consumed msg/s | 13,994.668 | 20,014.149 |
| Maximum publish errors/s | 0 | 0 |
| Publish p99 ms | 3,572.031 | 122.445 |
| Reported producer-delay p99 µs | 60,000,255 | 96 |
| End-to-end p99 ms | 3,555.007 | 5,046.015 |
| Maximum backlog | 253 | 50,238 |
| Final sampled backlog | 0 | 253 |

The candidate completed all 30 samples and kept up with the offered producer
rate. Consumer backlog rose sharply and later recovered, rather than growing
throughout the run; its mean rate also kept up. However, end-to-end tail latency
**regressed**, and this is not an acceptable latency result or Kafka parity.
No warnings, exceptions, or errors appear in the client log. Only the initial
group join is logged; a logged rebalance does not explain the delay. These are
single trials with no matched Kafka 20k measurement yet.

### Post-run diagnostics

After both runs and all clients stopped, the existing database was restarted
without a gateway. The checked-in profiling SQL ran, then the database stopped
cleanly again. Profiles are preserved in `parallel4-profile-*.txt`.

- The latest 100 batches in partition 0 averaged 225,782 logical bytes and 4,168
  stored bytes, using PostgreSQL pglz compression (about 54:1). Disabling storage
  compression would have a substantial space/I/O tradeoff for this payload; the
  observation alone does not establish its CPU cost.
- The write diagnostic copied one batch 100 times into a new topic and rolled
  back everything. Total query execution was 338.398 ms. Across creation and
  appends, pgrx `broker_call` self time was 204.859 ms; nested `broker.dispatch`
  consumed 132.546 ms. This points to the Rust/SPI boundary as a useful target,
  without isolating CRC validation, JSON conversion and allocation individually.
- Ten bulk tail fetches took 527.429 ms. Inner partition dispatch accounted for
  109.784 ms across 160 calls, while the bulk helper and public SQL wrapper
  accounted for 414.705 ms of self time. The bulk response construction/copying
  path warrants profiling and a binary-result alternative.

These profiles exclude client/network costs and write commit durability, use
function tracking, and are not throughput benchmarks. They do not prove the
cause of the intermittent consumer delay. The next experiment should reduce
record-byte JSON/hex conversion while retaining the current correctness tests
and durability policy, then repeat matched workloads and inspect consumer tails.

The parallel-produce source archive is `.dev/omb-parallel4-source.tar.gz`,
SHA-256 `b20cd6c67944e798f2d9667307e32d723422e2e342814c24f4d970f663346a5d`. Builder Cargo manifests and Cargo.lock match the local
pinned source; hashes are in `parallel4-build-manifest-hashes.txt`.


## Extension 0.1.4 binary-fetch follow-up

The candidate returns record batches as bytea rows instead of building a
hex/JSON response. It retains the same snapshot, first-batch exception and
shared byte budget, with explicit result ordering and bounded work_mem for
sorts/materialization. The four-partition producer concurrency and write path
are unchanged. Regression validation passed 37 integration tests, Clippy with
warnings denied, formatting and both CNPG Kustomize renders.

The 10k run completed 30 samples at about 10,006 msg/s, with zero publish errors.
End-to-end p99 was 30 ms, versus 39 ms for the prior candidate and 10 ms for
Kafka. This is still outside the working latency target and remains a single
trial. The matched 20k run is also complete, as reported below.
The isolated VM was restarted specifically for these measurements.


### Binary-fetch 20k result

| Metric | JSON fetch / parallel produce | Binary fetch / parallel produce |
|---|---:|---:|
| Mean published msg/s | 20,014.233 | 20,013.521 |
| Mean consumed msg/s | 20,014.149 | 20,013.092 |
| Maximum publish errors/s | 0 | 0 |
| Publish p99 ms | 122.445 | 78.828 |
| End-to-end p99 ms | 5,046.015 | 88.000 |
| Reported producer-delay p99 µs | 96 | 82 |
| Maximum / final sampled backlog | 50,238 / 253 | 506 / 0 |

Both current runs contain all 30 measurement samples and no warning, error or
exception entries in the client logs. The 20k candidate kept up with the offered
rate and avoided the prior run's large backlog and multi-second tail. This is
a single-trial improvement; repeated trials and a matched Kafka 20k measurement
are still missing. At 10k, 30 ms remains three times Kafka's 10 ms reference.
The working parity target is not met.

After all clients and services stopped, a database-only restart verified SQL
version 0.1.4, permission for the binary read API, and enabled fsync, full-page
writes and synchronous commit. All 16 partitions had contiguous batch ranges
and counters matching their last batch endpoints. Verification output is in
`binary014-storage-verification.txt`. The database then stopped cleanly.

Deployed gateway source and SQL migration hashes match the local tested source;
the builder's Cargo manifests and lockfile also match. Evidence is preserved in
`binary014-deployed-hashes.txt`, `binary014-build-manifest-hashes.txt` and
`binary014-source-hashes.json`. The pgrx shared library is unchanged from the
previous candidate; only the gateway and extension SQL changed.

The binary-fetch source archive is `.dev/omb-binary014-source.tar.gz`, SHA-256
`92d1a1d713948436e7e043787e28bd01d70d22b4f81ff72532b16118712053cb`.

After 0.1.4 evidence collection, the isolated VM was verified STOPPED through
the OCI API. The 80 GiB boot volume and dedicated network remain for continuing
the active performance goal. The native PostgreSQL test cluster is also stopped.


## Extension 0.1.5 binary-write follow-up

The validating pgrx write entry point now receives bytea records and passes raw
batch bytes through typed SPI parameters. Metadata remains derived from validated
records. Gateway size checks remain, while full record/CRC validation occurs once
at the authoritative storage boundary. These changes were measured together;
the run does not isolate the gain from each. The four-partition concurrency,
0.1.4 binary read path, workload, resource caps and durability settings remain.

| Metric | 10k offered | 20k offered |
|---|---:|---:|
| Mean published msg/s | 10,008.185 | 20,023.932 |
| Mean consumed msg/s | 10,008.016 | 20,023.579 |
| Maximum publish errors/s | 0 | 0 |
| Publish p99 ms | 10.595 | 13.465 |
| End-to-end p99 ms | 16.000 | 20.000 |
| Reported producer-delay p99 µs | 151 | 174 |
| Maximum / final sampled backlog | 108 / 51 | 210 / 106 |

Both runs contain 30 samples and clean client logs. The 10k end-to-end p99 improved
from 30 ms to 16 ms; Kafka's same-load reference remains 10 ms. The 20k p99 improved
from 88 ms to 20 ms. Final sampled backlog is not zero and does not establish a
fully drained run. These are single trials, not proof of parity: the 10k latency
target is still missed, and matched higher-load Kafka results, repetitions,
capacity/scaling and RF=3 acceptance remain pending.

The native suite passed 40 tests, including malformed later-batch rejection,
rollback without offset gaps, an injected database failure on the second valid
batch that rolled back the first batch, and restricted-role fallback/grants.
Workspace Clippy and both deployment manifest renders passed.

A post-run database-only restart verified 0.1.5, binary API permissions, enabled
fsync/full-page-writes/synchronous-commit, and contiguous batch ranges with matching
counters across all 16 partitions. Gateway, extension source and SQL hashes match
the tested local source; builder Cargo manifests and lockfile match too. This
variant rebuilds both gateway and pgrx library. See `binary015-*.txt` and
`binary015-source-hashes.json` for evidence.

The read-only empty-fetch diagnostic made 100 calls covering 16 partitions each.
Total query execution was 65.346 ms and function execution was 62.873 ms, or about
0.63 ms per call. It excludes gateway/network costs and is not a throughput test.
This does not by itself explain the full remaining latency gap; gateway database
round trips and client-visible timing warrant further measurement.

The binary-write source archive is `.dev/omb-binary015-source.tar.gz`, SHA-256
`cdf2fe9f0453db777e9e67d75c993a9d82b65bfd21b1316987b975ebec314604`.

After 0.1.5 evidence collection, the OCI API confirmed the isolated VM STOPPED.
The 80 GiB boot volume and dedicated network remain for the active benchmark
goal. The native PostgreSQL test cluster is also stopped.


## Prepared statement reuse (extension 0.1.5)

The gateway now reuses prepared statements for its finite set of fixed SQL
queries through deadpool-postgres's per-connection cache. The extension library
was byte-for-byte identical to the binary-write baseline (verified with cmp).
Isolation, commit confirmation, primary checks and workload settings are unchanged.

One matched 10k run completed all 30 samples with clean client logs:

| Metric | Prepared statements | Previous binary-write run |
|---|---:|---:|
| Mean published msg/s | 10,007.180 | 10,008.185 |
| Mean consumed msg/s | 10,007.004 | 10,008.016 |
| Publish errors/s maximum | 0 | 0 |
| Publish p99 ms | 10.126 | 10.595 |
| End-to-end p99 ms | 15.000 | 16.000 |
| Producer-delay p99 µs | 134 | 151 |
| Maximum / final sampled backlog | 103 / 53 | 108 / 51 |

This small single-trial difference does not establish a repeatable gain. The
15 ms end-to-end p99 remains 1.5 times Kafka's 10 ms reference and misses the
working 1.25-times gate. Repeated trials, higher-load references and distributed
RF=3/scaling acceptance remain pending; no capacity or parity claim is made.

All 40 native integration tests passed in 71.54 seconds. A post-run database
restart verified synchronous commit, fsync, full-page writes, permissions, and
contiguous batch ranges/counters across all 16 partitions. Deployed gateway and
extension source hashes match the locally tested code. See `cached015-*.txt` and
`cached015-source-hashes.json`. Raw logs and workload sidecars are retained in
`.dev/omb-evidence/results/`. The database stopped cleanly after verification.

Source archive: `.dev/omb-cached015-source.tar.gz`, SHA-256
`f2b068d22d909332a11053af4fd0d3f9bb453ca611044bcc8853376839f554ff`.

After the prepared-statement run, OCI confirmed the isolated VM STOPPED.
Its boot volume and dedicated network remain retained; native PostgreSQL is stopped.


## Higher-load matched comparison (32k)

The cached015-32k-1 pair uses the recorded, hash-verified prepared-statement
binaries without rebuilding. The new `benchmarks/omb/oci-pair.sh` runner compares
Kafka then the broker at 32,000 offered messages/s, each on a fresh store with
the same resource caps. The Kafka reference completed 30 samples at 32,027.431 published and consumed
messages/s, with zero publish errors, clean client logs, 11.573 ms publish p99
and 12 ms end-to-end p99. Maximum sampled backlog was 170 and final backlog zero.
Broker measurement is complete; the paired results are below. Remote evidence directory:
`/home/ubuntu/results/omb-pair-cached015-32k-1/`.

Kafka's transient unit reports exit-code 143 after the runner issued its stop
command following completed measurement. The journal shows Kafka closing its
event queues, metrics and server registrations before exit at 17:36:03 UTC.
This is preserved as a post-measurement shutdown-status detail, not hidden as a
clean systemd exit. Applied limits were CPUs 0–1 and 9 GiB for Kafka, versus the
same CPUs and 8 GiB PostgreSQL plus 1 GiB gateway for the broker.

| Metric | Kafka 3.6.1 | Broker cached015 |
|---|---:|---:|
| Mean published msg/s | 32,027.431 | 32,022.299 |
| Mean consumed msg/s | 32,027.431 | 32,021.814 |
| Publish errors/s maximum | 0 | 0 |
| Publish p99 ms | 11.573 | 18.046 |
| End-to-end p99 ms | 12.000 | 27.000 |
| Producer-delay p99 µs | 475 | 208 |
| Maximum / final sampled backlog | 170 / 0 | 320 / 146 |

Both runs contain 30 samples and clean client logs. The broker kept up with
the offered rate in this trial but its end-to-end p99 was 2.25 times Kafka's,
missing the 1.25-times target. Final broker backlog was not zero; this is not
proof of drain or maximum sustainable capacity. Repetitions and RF=3/scaling
acceptance remain pending. The comparison utility verified matching workload,
client and upstream revision sidecars. Full evidence is in `paired32k-1/`.

After traffic ended, PostgreSQL restarted and verified all 16 contiguous
partition logs/counters plus enabled fsync, full-page writes and synchronous
commit. Binary hashes matched the previously verified cached015 deployment.
All benchmark services stopped after checks.

A separate temporary-table diagnostic copied 100 real batches (17,606,837
logical bytes). PGLZ insert execution was 121.788 ms versus 42.207 ms for
uncompressed EXTERNAL storage. Relation sizes were 483,328 versus 18,374,656
bytes; record reconstruction was 2.174 versus 10.415 ms. All 100 copied batch
values were identical. The transaction rolled back, leaving broker settings
and data unchanged. This excludes durable WAL/commit cost, and cache/order
effects remain; it is not an end-to-end performance result. These tradeoffs
motivate testing LZ4 compression rather than assuming no compression is faster.

The VM remains running for an isolated LZ4-enabled PostgreSQL candidate build,
started only after all benchmark traffic and storage diagnostics stopped.
Docker adds liblz4 build/runtime support; default TOAST compression remains PGLZ.
No LZ4 performance or correctness result is claimed yet. Source build archive
`.dev/lz4-build-source.tar.gz` has SHA-256
`a9bbf77ccf13fef2919305596068c729ae92b12de51f76bbfa00b40570dea133`.


## LZ4 candidate: validation and measurement in progress

The candidate built as
`sha256:6821d31236114005fd88bc092f63721b540538169b8f048e09771ed68e24da19`.
The native suite passed 41 tests in 72.91 seconds, with LZ4 required, including
mixed PGLZ/LZ4 record integrity, rollback, immediate database crash and committed
offset resume. Native PostgreSQL is stopped.

The same-image, fresh-store PGLZ/LZ4 32k comparison has started under
`/home/ubuntu/results/omb-compression-lz4015-32k-1/`. It changes only record-column
compression and verifies the actual stored method after restart. Results are
pending; no LZ4 performance gain is claimed. The isolated VM remains running.

The same-image PGLZ phase completed 30 samples at 32,022.753 published and
32,022.254 consumed messages/s, with zero publish errors and clean client logs.
Publish p99 was 17.557 ms; end-to-end p99 was 44 ms; producer-delay p99 was
198 µs. Maximum/final sampled backlog was 886/150. Restart, partition-range
and actual-compression checks passed; relation storage was 304,529,408 bytes.
LZ4 measurement is now live. The PGLZ p99 differs from the earlier 27 ms trial,
so the same-image comparison and repetitions are necessary; do not substitute
the older trial as this experiment's control.


### Completed same-image compression pair

| Metric | PGLZ | LZ4 |
|---|---:|---:|
| Mean published msg/s | 32,022.753 | 32,021.118 |
| Mean consumed msg/s | 32,022.254 | 32,019.942 |
| Publish errors/s maximum | 0 | 0 |
| Publish p99 ms | 17.557 | 12.238 |
| End-to-end p99 ms | 44.000 | 20.000 |
| Producer-delay p99 µs | 198 | 201 |
| Maximum / final sampled backlog | 886 / 150 | 355 / 354 |
| Total batch relation bytes | 304,529,408 | 251,641,856 |

Both runs completed all 30 samples with clean client logs. Sidecars match for
workload, client settings and upstream revision; both used immutable image
`sha256:6821d31236114005fd88bc092f63721b540538169b8f048e09771ed68e24da19`.
Post-restart checks verified durability settings, contiguous offsets/counters
for 16 partitions, and the requested compression method: 67,854 PGLZ-compressed
batches and 69,922 LZ4-compressed batches, with no other compressed method.
Batch boundaries differ with producer timing, so storage sizes are observed run
results, not a fixed-input codec ratio. Full evidence is in `compression32k-1/`.

LZ4's observed end-to-end p99 is 45.45% of this PGLZ trial, but still 1.67 times
the separate Kafka 12 ms reference. Final LZ4 backlog is nonzero. These single
trials do not establish repeatability, parity or maximum capacity. The default
compression policy remains unchanged. Both services stopped after checks.

A separate ordered-settings pipeline gateway candidate passed 42 native tests
in 75.39 seconds, including an explicit BEFORE INSERT check of transaction
settings with contrary connection defaults. It was not part of this compression
pair and has no performance result yet. The native database is stopped.


The next gateway comparison began on 2026-11-03 UTC; see
[the next report](../2026-11-03/README.md).
