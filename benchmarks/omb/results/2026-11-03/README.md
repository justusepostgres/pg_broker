# Broker performance experiments — 2026-11-03

Status: three fresh RF=1 32k pairs and one 50k pair met the p99 target, but rarer broker stalls remain. Full performance/scaling acceptance remains open.

### Three uninstrumented WAL-off pairs verified

The sequence completed at 21:52:12 UTC; authoritative observation confirmed
the original parent inactive/dead, MainPID 0 and Result success. Full pair and
parent evidence is retained in `waloff32k-{1,2,3}/` and `waloff32k-run/`.
`waloff32k-comparison.json` passes all captured-environment, fresh-store,
durability, matching-artifact and clean-restart checks.

| Trial | Kafka / broker p99 ms | p99.9 ms | p99.99 ms | Maximum ms | Broker peak / final sampled backlog |
|---|---:|---:|---:|---:|---:|
| 1 | 19 / 14 | 107 / 40 | 160 / 85 | 196 / 110 | 178 / 0 |
| 2 | 13 / 13 | 31 / 30 | 41 / 48 | 48 / 53 | 224 / 0 |
| 3 | 13 / 13 | 41 / 28 | 166 / 41 | 240 / 52 | 184 / 178 |

Every system completed 30 samples at about 32k publish/consume msg/s with zero
publish errors. Only Kafka trial 1 had client warnings: two startup retries
before warmup. Eight unclassified gateway deadline messages remain in each
broker log, with no ERROR/FATAL matches in the retained gateway/Kafka server
logs. All samples, tails and residual backlogs are retained. The fixed-load p99
target holds across three trials; this does not prove maximum capacity, zero
residual backlog, all-tail superiority, RF=3, or distributed scaling. The tested
candidate explicitly uses `wal_init_zero=off`; no production default changed.

### Separate-host comparison now running

**Completed:** the full pair finished at 22:11:18 UTC. The retained server unit
had MainPID 0, SubState exited, Result success and ExecMainStatus 0 under the
original invocation. `external32k-1/` contains the complete server and client
evidence; `external32k-comparison.json` passes captured-environment and placement
verification, including matching Java/resources/scripts/payload, copied results,
fresh stores, durable SQL state and both restart checks.

| Metric | Kafka | Broker |
|---|---:|---:|
| Mean publish msg/s | 32,026.552 | 32,026.164 |
| Mean consume msg/s | 32,027.098 | 32,025.627 |
| End-to-end p99 ms | 20 | 14 |
| End-to-end p99.9 ms | 43 | 46 |
| End-to-end p99.99 ms | 127 | 110 |
| End-to-end maximum ms | 154 | 131 |
| Peak / final sampled backlog | 195 / 0 | 177 / 162 |

Both systems completed all 30 samples with zero publish errors and client
warning/error matches. Eight unclassified gateway deadline messages remain;
retained gateway and Kafka server/recovery logs have no ERROR/FATAL matches.
This one separate-host pair meets the fixed-load p99 target, but its broker
p99.9 is slightly worse and its final sampled backlog is nonzero. It does not
establish repeatability, maximum capacity or replicated/distributed scaling.

**Next load:** `omb-external-017-50k-1.service`, invocation
`b0d754c2c6e644b3a23b9adf5a4d629f`, is now running a fresh broker-first 50k pair
with one-minute warmup and 15-minute measurement per system (90 samples each).
Four old, completed Kafka scratch stores were reclaimed only after their
retained local and remote artifacts matched and the host was idle; all raw
results remain. Free space before launch was 78,435,950,592 bytes. The record
is `/home/ubuntu/results/reclaimed-for-external50k-1.json`.

The same runner hash `81fbcf99...`, runtime image, pipeline gateway, SQL 0.1.7,
LZ4 and explicit WAL-off policy are retained. Broker client invocation
`fd497ccc00ef4b86adb1b9e6a6360a3d` was confirmed active at 22:14:47 UTC, with
warmup starting 22:14:28. Its first warmup window retained 179.6 ms publish p99
and 445.1 ms maximum, near 50k messages/s with zero publish errors. No completed
50k external-client result is available yet. Default-flush Kafka references,
higher-rate exploration and the full scaling gates remain outstanding.

Update at 22:04:19 UTC: the Kafka client phase completed all 30 samples at
32,027 publish/consume msg/s with zero publish errors or client warnings.
End-to-end p99/p99.9/p99.99/max were 20/43/127/154 ms; peak/final backlog was
195/0. Its finalized evidence was collected and atomically handed to the
waiting server runner. Kafka's clean-restart offset check passed before the
broker phase request appeared. Raw interim client evidence and checksums are
in `external32k-kafka-interim/`.

Broker client invocation `bbc10ea709f046e5ad680ef002c73803` is active with PID
7083. Its actual metadata reports leader `10.84.0.247:9092`; the subscribed
consumer group became ready and produce/fetch readiness succeeded. Warmup
started at 22:03:57 UTC. Its first warmup window retained 58.2 ms publish p99
and 126.3 ms maximum, with no publish errors. The broker measurement, its
restart checks and the completed matched comparison remain pending.

After the original sequence completed, the checksummed raw/recovery evidence
for old `confirm32k-2` was matched locally and remotely, and only its disposable
Kafka data directory was reclaimed under the idle-host lock. About 40 GiB was
then free. Its evidence remains retained; the cloud reclamation record is
`/home/ubuntu/results/reclaimed-confirm32k-2-store.json`.

The new server-runner archive matched SHA-256
`88f27c7440d530d1e98aff028df60acef7ca047c6f6401b3538f7fbd88662dff` before installation.
One temporary firewall rule permits only client `10.84.0.134/32` to server
`10.84.0.247/32` TCP ports 9092/19092. The OCI security list was not changed.
Server job `omb-external-017-32k-1.service`, invocation
`55fa6a7ef986426796d7b2fe7fb87a26`, is running a fresh Kafka-first 32k pair with
explicit WAL-off and the existing 2-CPU server resource/durability contract.

The separate client uses two CPUs, a 2-GiB cgroup limit and 1-GiB Java heap;
its OCI shape has a 2 Gbit/s network limit, captured in
`client-host-setup/instance-profile.json`. Client invocation
`c9930283d9264ce7bce2c74d99ec9c82` has discovered leader `10.84.0.247:19092`,
joined the subscribed group, and started warmup at 21:56:03 UTC. Its first
warmup window retained a 330.2 ms publish p99 spike. No measured result or
broker phase is complete yet. These separate-host results must not be pooled
with loopback trials. The final client-launcher preflight and source checksums
are retained in `client-host-setup/final-preflight/`.

### Second uninstrumented WAL-off pair complete

Trial 2, broker-first, completed at 21:39:07 UTC. The combined
`waloff32k-two-pairs.json` passes captured-environment consistency, fresh-store,
durability and restart checks. All raw artifacts and checksums are retained in
`waloff32k-2/`. Both systems completed 30 samples with zero publish errors or
client warnings, and zero final sampled backlog.

| Metric | Kafka | Broker |
|---|---:|---:|
| Mean publish msg/s | 32,024.111 | 32,021.023 |
| End-to-end p99 ms | 13 | 13 |
| End-to-end p99.9 ms | 31 | 30 |
| End-to-end p99.99 ms | 41 | 48 |
| End-to-end maximum ms | 48 | 53 |
| Peak sampled backlog | 193 | 224 |

The third pair was confirmed running under the original parent invocation at
21:40:13 UTC. Two completed trials do not establish the three-trial result or
maximum capacity. The broker's p99.99 and maximum remained slightly worse in
this pair, despite similar p99/p99.9; those tails are retained.

The separate-client server handoff and placement verifier are now prepared
locally. Twenty-seven analysis tests pass, including a lost-delivery-response
test that reuses exact archive bytes without launching another client. Existing
completed 32k, 50k and WAL-off evidence still validates. These tests do not prove
live separate-host broker operation; no modified server runner or network
configuration has been applied to the ongoing sequence.

### First uninstrumented WAL-off pair complete

Trial 1 completed at 21:26:03 UTC; the original repetition unit remains active
for the remaining pairs. `waloff32k-1/` retains all raw evidence and local
checksums; `waloff32k-one-pair.json` passed captured-environment, durability,
SQL provenance and restart validation. The captured runner still has its
original `fecca25f...` hash; local preparation for 75k has not been uploaded.

| Metric | Kafka | Broker |
|---|---:|---:|
| Mean publish msg/s | 32,023.236 | 32,027.624 |
| End-to-end p99 ms | 19 | 14 |
| End-to-end p99.9 ms | 107 | 40 |
| End-to-end p99.99 ms | 160 | 85 |
| End-to-end maximum ms | 196 | 110 |
| Peak / final sampled backlog | 159 / 0 | 178 / 0 |

Both completed 30 samples with zero publish errors. Kafka retains two startup
WARN lines for retried NOT_LEADER_OR_FOLLOWER responses before warmup, plus INFO
group-join retries. Broker client warnings were zero. Eight gateway deadline
messages remain in the raw log; this original binary does not identify their
phase. The separately traced diagnostic's idle-header finding must not be
assumed to prove the cause of every expiry here. No server ERROR/FATAL matches
were found in the retained gateway and Kafka logs. This is one fixed-load pair,
not proof of repeatability, capacity, or a causal gain from WAL initialization.

### Separate client prepared

A dedicated two-CPU / 12-GiB Ampere client VM was provisioned in the benchmark
VCN with a 60-GiB boot disk. Its bounded OMB build completed successfully at
21:26:14 UTC. Verified clean upstream revision:
`5b1fa70951a323da26bd587174b58bb2c65b0b5c`; Kafka client dependency: 3.6.1.
Java 17.0.20.1 matches the existing host, but must be selected explicitly through
PATH because the new VM's default Java is 21. The retained build log is in
`client-host-setup/`. No benchmark has run from this client, and no broker
network ports were opened. A separate-host paired comparison remains pending.

The new client workload launcher then passed a real traffic-free systemd
preflight: CPUs 0–1, 2-GiB memory limit, 1-GiB Java heap and 1,600-second runtime
bound, with exact Java/upstream/payload/script provenance captured. The remote
controller retained and collected invocation `ec20aa0a71e04d729b1ed323219879f3`,
then successfully collected the same invocation again without relaunching it.
Artifacts are in `client-host-setup/preflight/`; script checksums match. An
earlier controller preflight lost its short-lived service handle to systemd
garbage collection, which the retained-after-exit service setting corrected.
These are setup checks, not broker throughput or network-recovery results.

The local evidence validator now rejects malformed required metrics and checks
that diagnostic measurement-log counts and timestamp order match the result,
including midnight rollover. All 21 analysis tests pass. Revalidation of both
completed WAL diagnostic pairs, the three 32k pairs and the completed 50k pair
passed against retained raw artifacts; the recorded performance conclusions
are unchanged.

### Completed opposite-order WAL repeat

The off-first pair completed at 21:11:04 UTC and passed the same configuration,
process-crash redo, endpoint and contiguous-log checks. Both phases sustained
about 32,024 msg/s with zero publish errors or client warnings. Full evidence
and checksums are in `walinit-2/`; the interim capture below is superseded.

| Metric | Zero initialization on | Off |
|---|---:|---:|
| Publish p99 ms | 9.717 | 10.664 |
| End-to-end p99 ms | 13 | 14 |
| End-to-end p99.9 ms | 59 | 41 |
| End-to-end p99.99 ms | 78 | 164 |
| End-to-end maximum ms | 95 | 212 |
| Measured commits over 10 ms | 35 | 50 |
| Measured commits over 40 ms | 21 | 5 |
| Maximum logged measured commit ms | 56.761 | 96.594 |
| Peak / final backlog | 188 / 188 | 210 / 148 |

Both orders reduced p99.9 and the frequency of larger commits, but the second
off-run worsened rarer quantiles and maximum latency. Do not describe the
candidate as uniformly faster or discard its burst. Three fresh uninstrumented
Kafka/candidate pairs with alternating order are now running, using the original
pipeline gateway and explicit `wal_init_zero=off`. No deployment default has
changed. The 46-test native correctness run with this setting passed separately.

### Reversed-order WAL repeat — off phase complete

The fresh off-first phase passed its SIGKILL/redo, offset and contiguous-storage
checks at 21:04:29 UTC. It sustained 32,024 msg/s with no publish errors/client
warnings. End-to-end p99/p99.9/p99.99/max were 14/41/164/212 ms; peak/final
backlog was 210/148. All 30 samples remain included. Raw interim evidence is in
`walinit-off-repeat-interim/`.

This run retained one larger burst: five commits over 40 ms clustered between
21:01:30.006 and 21:01:30.110 UTC, with a 96.594 ms maximum. That window reached
212 ms end-to-end latency. Thus the candidate did not eliminate all larger
stalls, despite the lower p99.9 than the first on baseline. Nearby one-second
host samples show about 2% steal and 7–9% I/O wait, without establishing a cause.
The same-pair on baseline is still running; this is not yet a complete repeat.

### Controlled WAL initialization pair

The on-first pair completed at 20:54:08 UTC. Both fresh stores used the same
tracing binary, resources, client and instrumentation; the verifier confirmed
that only the requested WAL initialization setting differed. Both PostgreSQL
SIGKILL/restart checks preserved every partition endpoint and contiguous log.
Each run produced 30 samples with zero publish errors or client warnings.
Artifacts, all measured slow SQL and checksums are retained in `walinit-1/`.

| Metric | Zero initialization on | Off |
|---|---:|---:|
| Mean publish msg/s | 32,023.211 | 32,020.892 |
| Publish p99 ms | 9.562 | 10.336 |
| End-to-end p99 ms | 13 | 13 |
| End-to-end p99.9 ms | 59 | 33 |
| End-to-end p99.99 ms | 93 | 55 |
| End-to-end maximum ms | 115 | 78 |
| Measured commits over 10 ms | 46 | 65 |
| Measured commits over 40 ms | 24 | 0 |
| Maximum logged measured commit ms | 81.385 | 25.942 |
| Peak / final backlog | 186 / 0 | 195 / 0 |

This supports zero filling as a contributor to the larger periodic commit
stalls on this host. It does not establish universal causation or eliminate all
tail events. Tradeoffs remain: publish p99 increased, more commits exceeded
10 ms, and cumulative WAL sync time increased from 100,837.639 to 138,844.102 ms
(146,561 / 145,122 sync calls; counters include warmup and post-workload time).
A reversed-order repeat and uninstrumented Kafka comparisons are needed before
changing a deployment default. Durability settings stayed enabled throughout.

The comparison verifier was exercised against finalized baseline artifacts and
rejected altered copies with weakened durability, lost offsets, missing redo
evidence or missing SQL provenance. Temporary altered copies were discarded.

### Server diagnostic: stalls localized to commit

The instrumented broker run completed at 20:36:25 UTC with 30 samples, zero
publish errors, 32,022 publish/consume msg/s, end-to-end p99/p99.9/max of
12/61/90 ms, peak backlog 186 and final backlog 0. Restart, contiguous-range and
LZ4 checks passed. Complete artifacts and checksums are in `server32k-1/`.

All 46 PostgreSQL statements exceeding the 10 ms logging threshold were
`COMMIT`; 42 occurred after measurement started. The >40 ms commit events form
clusters with median spacing 21.114 seconds. Retention scans completed every
roughly 30 seconds, and the traced connection expiries were all frame-length
reads (idle or incomplete headers), not active handler/payload/write deadlines.
This narrows the investigation beyond the previously suspected retention cycle.

WAL counters increased by 286,020,575 bytes over 387.417 seconds, including
warmup and post-workload shutdown time. That rate corresponds to a 16 MiB WAL
segment every 22.725 seconds; using only traffic time brings it closer to the
observed 21-second pattern. This is circumstantial evidence for segment
initialization, not observed segment-boundary attribution. Post-run settings
confirmed `wal_init_zero=on`, `wal_recycle=on`, 16 MiB segments and `fdatasync`,
with fsync, full-page writes and synchronous commit all enabled. The temporary
post-run configuration inspection stopped the database again afterward.

The next controlled experiment is fresh stores with WAL zero initialization
on/off, retaining the same binary, instrumentation and durability settings.
PostgreSQL documents [wal_init_zero](https://www.postgresql.org/docs/17/runtime-config-wal.html)
as controlling zero filling of newly created WAL files. No production default
has changed, and the cause remains unproven until the experiment is evaluated.

### Completed JVM diagnostic

The whole diagnostic pair completed at 20:23:07 UTC, passed both restart checks
and captured-environment validation, and recorded no client warnings or publish
errors. Full evidence, one unrotated GC log per client, the client log map,
comparison and per-window pause analysis are in `gc32k-1/`. The recorded
instrumentation marker explicitly excludes this pair from acceptance runs.

Kafka/broker sustained 32,024/32,023 publish msg/s, with end-to-end p99 13/13 ms
and p99.9 39/61 ms. Broker maximum end-to-end latency was 84 ms. Its 59 client
GC pauses and 78 safepoints had maxima of 4.560/4.768 ms. In the worst latency
window, the largest client safepoint was only 1.858 ms; in the next-worst
(83 ms maximum latency), it was 1.577 ms. A long client safepoint is therefore
not supported as the direct explanation for these stall magnitudes. This does
not exclude other client work or scheduling effects and does not identify a
server-side cause. The next diagnostic targets maintenance timing/database waits.

The earlier Kafka-only capture below remains historical interim evidence.

The separate 32k diagnostic preserves the original binary and workload, adding
bounded client JVM GC/safepoint logs. Kafka completed 30 samples at 32,024 msg/s
with zero publish errors, 13 ms end-to-end p99, 39 ms p99.9 and 218 ms maximum.
Across its measured interval, 54 client GC pauses and 76 safepoints were logged;
their maxima were 3.378 and 3.514 ms respectively. The worst latency window
contained a maximum safepoint of 3.402 ms. These observations do not support a
single long client safepoint as the direct explanation for the 218 ms event.
They do not exclude other client costs, server-side pauses, storage or scheduling
delays. GC and safepoint durations overlap and are not summed.

Finalized Kafka artifacts were captured only after that client JVM exited, in
`gc32k-kafka-interim/`, including raw logs, all 30 aligned windows and checksums.
The broker phase and whole-pair verification remain pending. This is diagnostic
evidence and is not added to the uninstrumented acceptance repetitions.

### Completed 50k pair

The broker-first 15-minute-per-system pair completed at 20:05:33 UTC. Both
90-sample results and captured-environment/restart checks passed validation.
Raw artifacts are in `capacity50k-1/`, with checksums; the full comparison is
`capacity50k-comparison.json`. The interim broker capture below is retained as
historical analysis, superseded by this complete pair.

| Metric | Kafka | Broker |
|---|---:|---:|
| Mean publish msg/s | 50,014.571 | 50,019.635 |
| Mean consume msg/s | 50,014.572 | 50,019.361 |
| Maximum publish errors/s | 0 | 0 |
| End-to-end p99 ms | 41 | 40 |
| End-to-end p99.9 ms | 208 | 304.001 |
| End-to-end p99.99 ms | 437.001 | 573.003 |
| End-to-end maximum ms | 530.003 | 642.003 |
| Publish p99 ms | 39.961 | 30.197 |
| Peak / final sampled backlog | 506 / 0 | 388 / 249 |

The p99 ratio is 0.976, meeting the fixed-load threshold for this single trial.
Broker p99.9 is 1.462 times Kafka. Kafka also had late measurement stalls; their
presence does not identify the cause of either system's stalls. Neither client
logged warnings/errors. The gateway's 18 connection deadlines remain in the raw
logs, as described below. This establishes neither repeatability at 50k nor
maximum capacity, RF=3 durability or distributed scaling.

### 50k broker phase — interim evidence

The broker completed its 15-minute measurement (90 samples) on the same 160 GiB
host, before the sequential Kafka reference. Mean publish/consume rates were
50,019.635 / 50,019.361 messages/s with zero publish errors. Sampled backlog
peaked at 388 and ended at 249 messages. Publish p99/p99.9 were 30.197/234.245 ms;
end-to-end p99/p99.9/p99.99/max were 40/304.001/573.003/642.003 ms. This preserves
the significant rarer stalls despite throughput keeping pace with the offered
load. It is not a maximum-capacity or comparative-performance result.

Post-restart checks passed for SQL 0.1.7, durable settings, permissions, all 16
partition ranges and actual LZ4 storage. Completed broker artifacts, process
cleanup sidecar and checksums are in `capacity50k-broker-interim/`; the Kafka
result, its restart check and whole-pair completion marker remain pending.
The client log contains no warning/error matches. The gateway log contains 18
`connection: deadline has elapsed` messages; these are retained even though a
literal ERROR/FATAL search does not match them. Its untimestamped messages alone
cannot distinguish idle-connection expiry from an active-request deadline.
As in earlier
runs, the wrapper terminated the upstream executor only after the complete
result was written (`post_result_cleanup=True`).

`capacity50k-broker-interim/window-analysis.json` retains all 90 measured
ten-second windows and their log timestamps. Twenty-four windows had publish
maxima above 100 ms. The largest publish maximum was 621.887 ms in window 2,
but window 83 still reached 277.183 ms publish / 615.003 ms end-to-end maximum.
Thus startup alone does not describe the observed stalls. Several spikes recur
around 30-second intervals; the gateway's retention loop also sleeps 30 seconds
between scans, making maintenance a diagnostic candidate alongside JVM pauses.
No contemporaneous maintenance timestamps were captured, so this is not causal
evidence. No windows are excluded from aggregate results, and window percentiles
are not averaged to manufacture aggregate percentiles.


The baseline/pipeline pair runs at 32k with identical LZ4 storage, fresh stores
and the same immutable runtime image. Only the candidate gateway executable
changes. Its SHA-256 is
`4c394f8a49e5a197600ef00a456e0f5d34d04939885a03d90d1fb1db33e4d8fc`,
matching the source that passed 42 native tests. Evidence is being collected at
`/home/ubuntu/results/omb-pipeline-015-32k-1/`. The results below supersede the initial pending status.

The preceding compression and Kafka reference evidence remains in
[the 2026-11-02 report](../2026-11-02/README.md). This run is a new trial;
prior latency figures are not substituted for its matched baseline.


| Metric | Previous gateway | Ordered-settings pipeline |
|---|---:|---:|
| Mean published msg/s | 32,026.225 | 32,022.555 |
| Mean consumed msg/s | 32,026.224 | 32,022.348 |
| Publish errors/s maximum | 0 | 0 |
| Publish p99 ms | 12.338 | 11.835 |
| End-to-end p99 ms | 20.000 | 19.000 |
| Producer-delay p99 µs | 201 | 204 |
| Maximum / final sampled backlog | 360 / 0 | 833 / 226 |

Both runs completed 30 samples with clean client logs. Matching workload/client
sidecars, image identity and resource caps are retained in `pipeline32k-1/`.
Post-restart checks verified all 16 contiguous logs/counters, durable settings,
and actual LZ4 compression in both stores. The new gateway source hashes match
the native-tested candidate; its 42-test result remains documented separately.

The candidate's p99 was 95% of this baseline, a small single-trial difference.
Its peak/final sampled backlog was higher, and 19 ms remains above the separate
Kafka reference of 12 ms (1.58x). This does not establish repeatability, parity
or maximum capacity. RF=3/distributed scaling acceptance also remains pending.

### Post-run diagnostics

Read-only cumulative database profiles were captured after all traffic stopped.
Both stores reported one timed checkpoint, roughly 72k partition updates (all
HOT), six autovacuums on the partition table, and zero full WAL buffers. I/O
and WAL timing collection were disabled, so zero timing counters cannot be
interpreted as zero I/O cost. Counters include startup, warmup and restarts,
not just the measurement window; they do not identify a latency-spike cause.

A separate read-only retention diagnostic exposed avoidable no-op work. On
the pipeline store with warm buffers, the existing boundary aggregate scanned
4,096 rows after a bitmap scan/sort, hit 2,063 buffers and took 6.465 ms. A
bounded first-live-row lookup used one index row, hit four buffers and took
0.032 ms. This supports implementing and testing a no-op retention early exit;
it does not establish an end-to-end gain or cover the all-expired path. Both
initial and warm diagnostic plans are retained. Services stopped after capture.

Source archive: `.dev/omb-pipeline015-source.tar.gz`, SHA-256
`9b4c012f0723fc0abb2bbc108c12f8c29476edb68f8c461f8726fc4efc87a41a`.

OCI confirmed the isolated VM STOPPED after evidence collection. Its boot
volume and network remain retained. Native PostgreSQL is also stopped.


## Retention early exit — SQL 0.1.6

The matched pair in `retention32k-1/` changes only the SQL retention dispatcher.
Both fresh stores use the same LZ4-enabled runtime image and the same verified
pipeline gateway executable. CPU, memory, workload, driver configuration and
upstream revision match. The baseline uses SQL 0.1.5; the candidate applies the
captured 0.1.5→0.1.6 migration. Native correctness verification passed 45 tests
in 75.63 seconds before this run.

| Metric | SQL 0.1.5 | SQL 0.1.6 |
|---|---:|---:|
| Mean published msg/s | 32,021.496 | 32,022.928 |
| Mean consumed msg/s | 32,021.497 | 32,023.473 |
| Publish errors/s maximum | 0 | 0 |
| Publish p99 ms | 11.673 | 11.468 |
| End-to-end p99 ms | 18.000 | 17.000 |
| Producer-delay p99 µs | 176 | 172 |
| Maximum / final sampled backlog | 346 / 0 | 338 / 0 |

Both runs completed 30 samples with zero client warning/error matches. After
restart, both verified durability settings, 16 contiguous partition logs and
actual LZ4 compression. The deployed SQL migration and captured runner are
byte-identical to the local sources. Both upstream processes required the
already-documented post-result executor cleanup; process sidecars preserve this.
The initiating SSH connection eventually reset, but the remote completion marker,
complete JSON/sidecars, post-restart checks and subsequent inspection establish
that the pair completed. It was not rerun or discarded.

The observed p99 ratio is 17/18 = 0.944. One trial does not establish a repeatable
gain. The candidate is still 1.42x the separate Kafka reference of 12 ms, above
the 1.25x target. This is not capacity, RF=3 or distributed scaling acceptance.

After all benchmark traffic stopped, rollback-only dispatcher diagnostics on
both stores measured the actual retain entry point. The second run, with warm
shared buffers but a fresh SQL session, took 9.531 ms / 2,636 buffer hits on
0.1.5 and 2.968 ms / 527 hits on 0.1.6. These include session/function startup;
they are single-call diagnostics, not isolated query-kernel timings or durable
throughput measurements. The transactions rolled back, and all containers were
stopped afterward. Both initial and warm plans and the diagnostic SQL are kept.

The next performance work is to locate the remaining producer-to-consumer delay
and repeat matched trials; the distributed-storage milestones remain separate
and unfinished. Do not infer them from this RF=1 result.

OCI confirmed the benchmark VM STOPPED after the retention trial and evidence
collection. Native PostgreSQL is also stopped. Boot storage/network remain retained.


## Caught-up fetch candidate — SQL 0.1.7 (native only)

A bounded binary-fetch early exit skips batch-index probes when the requested
offset equals the snapshot high watermark. The full 46-test suite passed in
76.64 seconds, including concurrent append/retention snapshot visibility and
the complete extension upgrade chain. No Kafka capability or durability rule
changes. The migration preserves existing grants.

`native-tail017-profile.json` retains all samples and buffer plans from a
sequential native before/after diagnostic on the same 16,000-batch fixture.
There were ten warmups followed by 100 samples for each position and version.

| Position across 16 partitions | 0.1.6 median ms | 0.1.7 median ms | Buffer hits before/after |
|---|---:|---:|---:|
| All caught up | 0.440 | 0.190 | 112 / 16 |
| Half caught up | 0.468 | 0.346 | 112 / 64 |
| All have data | 0.484 | 0.498 | 112 / 112 |

The data-bearing case was slightly slower in this trial. Timing includes the
native Python/SQL round trip, excludes Kafka/network/distributed load, and uses
sequential version order. This establishes less index work for caught-up reads,
not an end-to-end gain. The OCI `tail` experiment is prepared but has not run.
The last measured OMB candidate remains 0.1.6 at 17 ms p99. Both native PostgreSQL
and the OCI VM remain stopped at this milestone.

The matched OCI tail trial subsequently started as systemd unit
`omb-tail-017-32k-1.service` (invocation `e90ea882e7ce4830b0d871ce8730fcbe`).
It was confirmed active with baseline PostgreSQL/gateway containers running.
Results are pending at `/home/ubuntu/results/omb-tail-017-32k-1/`; the VM is
running for this experiment. Earlier stopped-state notes describe completed milestones.


## Completed caught-up fetch pair — SQL 0.1.6 / 0.1.7

The pending trial above completed at 2026-11-03 17:06:34 UTC. Both variants
used the same immutable LZ4 runtime image, verified pipeline gateway, fresh
stores, resource limits, upstream OMB revision and client/workload configuration.
The only SQL difference is the captured 0.1.6→0.1.7 fetch early exit. Both
migration files and the captured runner match the tested local sources exactly.
Raw evidence is in `tail32k-1/`; all 30 samples are retained for both variants.

| Metric | SQL 0.1.6 | SQL 0.1.7 |
|---|---:|---:|
| Mean published msg/s | 32,021.158 | 32,021.971 |
| Mean consumed msg/s | 32,021.159 | 32,022.000 |
| Publish errors/s maximum | 0 | 0 |
| Publish p99 ms | 280.871 | 9.242 |
| End-to-end p99 ms | 307.001 | 12.000 |
| Producer-delay p99 µs | 194 | 144 |
| Maximum / final sampled backlog | 350 / 0 | 191 / 158 |

The candidate reached the earlier Kafka reference's 12 ms p99 at this fixed load.
That is encouraging but does not establish parity: it is one candidate trial,
the Kafka reference is from a separate earlier run, and maximum capacity,
repeatability, RF=3 and distributed scaling have not been accepted.

The baseline experienced severe latency spikes around 16:56 UTC. Its full-run
p99 is much worse than the earlier SQL 0.1.6 result of 17 ms. The observed
candidate/baseline ratio is preserved in `comparison.json`, but must **not** be
presented as a causal 25x optimization. No samples were removed or replaced.
A brief read-only host snapshot taken during the baseline at 16:58:28 UTC,
after the spikes, reported roughly 2% CPU steal, 3–5% I/O wait, no swap use and
no memory pressure. It does not identify the spike cause. This additional
baseline-only diagnostic is retained and is another limitation of a causal
comparison; future matched repetitions should avoid mid-run diagnostics.

Both runs had zero client warning/error matches and passed post-restart checks
for durable settings, all 16 contiguous logs/counters and actual LZ4 storage.
The systemd job terminated successfully, all containers stopped, and evidence
was downloaded. The next acceptance step is fresh repeated Kafka/0.1.7 pairs
with the same resource and durability contract, followed by sustainable-capacity
and RF=3/distributed scaling work. The goal remains active.

OCI confirmed the VM STOPPED after the completed tail trial. Native PostgreSQL
is also stopped. Boot storage/network remain retained for the ongoing goal.


## Fresh Kafka repetitions — running

The previous goal turn produced a completed candidate result, but repeatability
remains unproven. The dedicated boot volume was expanded from 80 to 160 GiB,
keeping 10 VPUs/GiB. Ubuntu expanded its ext4 root partition automatically on
boot; `lsblk` reports 171,798,691,840 disk bytes and `df` reports 105 GiB free.
Volume size can change I/O performance, so the earlier 80 GiB Kafka reference
is historical and is not the acceptance baseline for the new repetitions.

Three fresh Kafka 3.6.1 / SQL 0.1.7 pairs are now running at 32k messages/s,
in Kafka-first, broker-first, Kafka-first order. Unit:
`omb-repeat-017-confirm32k.service`, invocation
`fa1da106469849d693bb1ec559221a65`, with a 65-minute runtime limit. The first
Kafka baseline was confirmed in warmup at 18:50 UTC. The fixed resource and
durability contract is unchanged apart from the common disk expansion. Each
pair records volume metadata, runtime limits, exact SQL migrations and image /
gateway identity, checks actual LZ4 storage and PostgreSQL offset continuity
after restart, and compares Kafka's 16 data-log endpoints across a clean restart.
No mid-run host diagnostics are scheduled. All stores/results are retained.

Results are pending under `/home/ubuntu/results/omb-pair-017-confirm32k-{1,2,3}/`.
The parent plan and completion markers are in
`/home/ubuntu/results/omb-repeat-017-confirm32k/`. Script checksums matched before
launch; shell syntax and all six comparison-analysis tests passed. The OCI VM
is running for this experiment; native PostgreSQL remains stopped. No new
performance result or parity claim is made until these trials are verified.

The repetition job was revalidated live at 18:58 UTC: the first Kafka run
finished, its 16 data-log endpoints matched across restart, and the first
0.1.7 broker measurement was running at roughly 32k messages/s without observed
publish errors. No complete pair or repeatability claim is made yet. Local
comparison analysis now has an opt-in captured-environment/restart verifier;
all nine regression tests passed. Remote benchmark scripts remain unchanged.


### First fresh Kafka pair completed; two repetitions pending

`confirm32k-1/` contains the first completed 160-GiB Kafka/0.1.7 pair. Both
variants completed 30 samples with zero publish errors and zero client warning /
error matches. All captured-environment checks passed, including the 16 Kafka
partition endpoints across restart, PostgreSQL durable log ranges and actual
LZ4. Both SQL migrations, the storage verifier and runner match local sources.
A checksum manifest preserves the downloaded evidence.

| Metric | Kafka 3.6.1 | Broker SQL 0.1.7 |
|---|---:|---:|
| Mean published msg/s | 32,027.083 | 32,022.575 |
| Mean consumed msg/s | 32,027.083 | 32,022.578 |
| Publish p99 ms | 13.335 | 9.557 |
| End-to-end p99 ms | 14.000 | 13.000 |
| Maximum / final sampled backlog | 20 / 0 | 182 / 154 |

The observed end-to-end p99 ratio is 13/14 = 0.929, within the 1.25 target for
this fixed-load pair. It does not establish repeatability or capacity. The second
pair has started broker-first and the third remains scheduled. The experiment
and OCI VM remain running; no overall parity claim is made from one pair.


### Second fresh pair completed; final repetition running

`confirm32k-2/` contains the broker-first pair. The combined
`confirm32k-two-pairs.json` passes captured-environment consistency checks.
Both variants completed all 30 samples with zero publish errors and passed
restart checks. Kafka/broker mean published rates were 32,023.890/32,020.840
msg/s and consumed rates were 32,024.824/32,020.378 msg/s. Publish p99 was
12.868/9.397 ms; end-to-end p99 was 13/12 ms (ratio 0.923). Peak/final sampled
backlog was Kafka 183/0 and broker 190/139.

Kafka logged two WARN lines at 19:10:07 UTC for a retried NOT_LEADER_OR_FOLLOWER
produce during topic startup. The log also records coordinator rediscovery
before successful group assignment. The two counted WARN lines precede the
19:11:10 measurement start; neither was discarded. The broker had zero client
warning/error matches. This pair is not described as having completely clean
startup logs. All raw samples and logs are retained, and exact SQL/runner bytes
match local sources. The third pair remains running; repeatability acceptance
is still pending its completion and verification.


## Three fresh 32k pairs verified

All three 160-GiB pairs completed, with alternating Kafka-first / broker-first /
Kafka-first order. `confirm32k-comparison.json` passes workload/client/revision
and captured-environment consistency validation across all three. Source SQL,
storage verifier, pair runner and repetition runner match local files. The
parent run and terminal state are retained in `confirm32k-run/`.

| Trial | Kafka end-to-end p99 ms | Broker end-to-end p99 ms | Broker / Kafka | Broker peak / final sampled backlog |
|---|---:|---:|---:|---:|
| 1 | 14 | 13 | 0.929 | 182 / 154 |
| 2 | 13 | 12 | 0.923 | 190 / 139 |
| 3 | 13 | 12 | 0.923 | 190 / 190 |

Both systems published and consumed approximately 32k messages/s in every run.
Every measurement completed all 30 samples with zero publish errors and no
client warning/error matches after measurement started. Kafka trials 2 and 3
each retain two startup WARN lines for retried NOT_LEADER_OR_FOLLOWER produces;
they occurred before warmup/measurement and are not hidden by the summary.
All gateway and Kafka server/recovery logs had zero ERROR/FATAL matches.
Kafka logs confirm version 3.6.1 and effective log.flush.interval.messages=1.
Both systems passed each run's clean-restart checks; PostgreSQL also verified
actual LZ4 storage and all 16 contiguous partition logs/counters.

This establishes the requested latency target for the tested **fixed 32k load**
across three RF=1 trials: 1-KiB upstream payloads, 16 partitions, one producer
and one subscribed consumer, identical client settings, same isolated VM/disk
and explicit resource/durability limits. It does not establish maximum
sustainable capacity, general Kafka parity, RF=3 or distributed storage scaling.
No result or startup warning was discarded. Native crash/fencing correctness
and historical CNPG HA evidence remain separate from these clean-restart runs.

### Next load: 50k messages/s — starting

The completed repetition job left all services stopped and 70 GiB free. A new
broker-first pair, `omb-capacity-017-50k-1.service`, has been launched using the
same runner, artifacts, 160-GiB volume and durability policy. Workload: one-minute
warmup plus 15-minute measurement per system (90 samples expected). Its 45-minute
runtime bound exceeds the normal paired duration. Evidence will be retained at
`/home/ubuntu/results/omb-pair-017-capacity50k-1/`. Results are pending; the VM
remains running for this work. Do not call a fixed 50k trial a maximum-capacity
measurement without further load exploration.

The 50k unit was confirmed active with broker traffic. Its early 19:33:52 UTC
window sustained roughly 50k/s without publish errors but reported 523.7 ms
publish p99. That spike is retained; no higher-load performance conclusion is
made before the complete 90-sample results and matched Kafka run are inspected.


## Tail-latency limitation found in the completed 32k evidence

The existing p99 result remains valid, but it must not be generalized to all
latency percentiles. Reanalysis of the original raw JSON now exposes p99.9,
p99.99 and maximum latency in `confirm32k-tail-comparison.json`.

| Trial | Kafka / broker end-to-end p99.9 ms | Kafka / broker p99.99 ms | Kafka / broker maximum ms |
|---|---:|---:|---:|
| 1 | 82 / 61 | 143 / 80 | 167 / 93 |
| 2 | 29 / 60 | 43 / 74 | 61 / 86 |
| 3 | 28 / 61 | 36 / 84 | 42 / 99 |

Thus the broker's p99.9 was about twice Kafka's in trials 2 and 3, despite its
better p99. The cause is not established. No raw samples, warnings or earlier
comparison artifacts were removed. The analysis now includes these rare-stall
metrics (and producer-delay units explicitly), reports absent legacy metrics as
null, and rejects invalid supplied values. All ten analysis regression tests
passed; running remote benchmark scripts remain unchanged.

The 50k job was revalidated active with approximately 50k messages/s and no
observed publish errors. Its early large spikes remain in the pending result.
After the complete higher-load pair is inspected, diagnostic runs should
correlate client JVM pauses and PostgreSQL/host waits with the rare stalls;
no causal diagnosis or general Kafka-performance parity is claimed yet.
