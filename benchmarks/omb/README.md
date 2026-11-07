# OpenMessaging Benchmark

This suite uses the upstream OpenMessaging Benchmark (OMB), pinned in `revision`,
and its unmodified Java Kafka 3.6.1 driver. No client protocol patches are used.
The [OMB documentation](https://openmessaging.cloud/docs/benchmarks/) describes
workloads, not a universal throughput target or an equivalent-hardware result.

## Initial gate: RF=1 compatibility and diagnosis

Use an isolated Linux host with JDK 17, Maven, Git and Python 3. Build the broker
with `cargo build --workspace --release --locked`; container builds now also use
release. `CARGO_PROFILE=release scripts/install-extension.sh` installs that build.
The default installer profile remains debug for existing development commands.

```sh
benchmarks/omb/build.sh
benchmarks/omb/run.sh smoke localhost:9092 /absolute/results/broker-smoke.json
benchmarks/omb/run.sh max-rate localhost:9092 /absolute/results/broker-max-1.json
```

`OMB_CHECKOUT` selects the upstream checkout directory. The runner refuses tracked
upstream changes, preserves driver/workload/revision alongside JSON and logs, and
bounds execution to 25 minutes, including an upstream topic-creation retry loop.
The pinned upstream Kafka topic creator leaves an executor alive after a run.
The wrapper allows ten seconds after a complete result JSON, then terminates the
remaining process group and records this cleanup in `.process.txt`. It never
changes benchmark code, shortens the measurement, or treats a missing result as
success.
The short smoke run includes one minute of warmup and one minute of measurement;
max-rate includes one minute of warmup and five minutes of measurement. The 50k
workload offers 50,000 messages/s for 15 minutes after warmup. Do not run it until
capacity and disk monitoring indicate that it is safe to do so.

All workloads have one topic, 16 partitions, 1 KiB values, one producer and one
consumer group with one consumer. The driver explicitly selects classic range
assignment, no compression, no idempotence, `acks=all`, one in-flight request,
5 ms linger and 256 KiB batches. This is a supported-feature comparison, not the
upstream exactly-once workload. Run the identical file against Kafka as well.
OMB uses subscribed consumers and asynchronous offset commits, unlike the earlier
custom sequential produce-then-fetch benchmark.

RF=1 and an empty topic configuration are deliberate: the current broker rejects
RF=3 and arbitrary Kafka topic configs. PostgreSQL standby copies do not turn that
into distributed partition replication. An RF=1 result cannot satisfy the
horizontal scaling or replicated-performance goal.

`FetchCloseReproduction.java` is a separate diagnostic for the INFO-level
invalid-full-fetch messages observed during shutdown. Compile it against the
unmodified Kafka client 3.6.1 and SLF4J 1.7.36 jars, then run its `main` method:

```sh
javac -cp '/path/to/jars/*' -d /tmp/fetch-close benchmarks/omb/FetchCloseReproduction.java
java -cp '/path/to/jars/*:/tmp/fetch-close' FetchCloseReproduction
```

It demonstrates that building an empty closing request changes validation of an
outstanding full response. It does not modify the OMB client, contact a broker,
or establish that the observed gateway response was correct. The analyzer counts
these diagnostics separately from warnings/errors, including those logged at INFO.
See Kafka's pinned [shutdown code](https://github.com/apache/kafka/blob/3.6.1/clients/src/main/java/org/apache/kafka/clients/consumer/internals/AbstractFetch.java)
and [fetch session handler](https://github.com/apache/kafka/blob/3.6.1/clients/src/main/java/org/apache/kafka/clients/FetchSessionHandler.java).

## Comparison contract

Record CPU architecture/count, RAM, storage size/performance, network topology,
software revisions, image digests, client placement and storage settings. Keep
producer configuration, workload, resources and placement identical. Run systems
sequentially on fresh data volumes, with no builds or other workloads competing.
Preserve errors, backlog, publish delay, end-to-end latency and publish latency;
producer throughput alone can hide a consumer that cannot keep up.

PostgreSQL uses `fsync=on`, `full_page_writes=on`, `synchronous_commit=on`.
Kafka's default `acks=all` does not promise a local disk flush per acknowledgment.
A local-durability comparison must separately record a Kafka flush policy such as
`log.flush.interval.messages=1`. Report a default-Kafka reference separately; do
not silently weaken PostgreSQL durability to improve a score. Neither RF=1 test
covers host loss.

The pinned Kafka 3.6.1 implementation defines its default flush-message and
flush-time intervals as `Long.MAX_VALUE` in
[LogConfig.java](https://github.com/apache/kafka/blob/3.6.1/storage/src/main/java/org/apache/kafka/storage/internals/log/LogConfig.java),
and [KafkaConfig.scala](https://github.com/apache/kafka/blob/3.6.1/core/src/main/scala/kafka/server/KafkaConfig.scala)
uses those defaults for the broker settings. Our explicit
`log.flush.interval.messages=1` reference is therefore a deliberate local-flush
comparison, not Kafka's default operating policy. A separate default-flush
reference still needs to be measured and clearly labeled before making broader
Kafka performance claims. PostgreSQL durability must remain enabled in that
reference; differences in acknowledgment guarantees must stay visible. No
currently running reference configuration was changed.

The locally prepared pair runner now accepts
`OMB_KAFKA_FLUSH_POLICY=per-append|background`, defaulting to `per-append`.
`background` explicitly sets the Kafka 3.6.1 default flush-message interval;
it leaves PostgreSQL fsync, full-page writes and synchronous commit enabled.
The selected mode is retained in `kafka-flush-policy.txt`. Environment verification
requires the requested value, absence of timer overrides, and matching effective
settings in both Kafka startup and restart logs. Analysis labels the background
reference's different local acknowledgment durability and rejects pooling the
two modes even without full environment verification. Historical explicit
`log.flush.interval.messages=1` captures remain identifiable from their properties.

Twenty-nine analysis tests pass, including mixed-mode and contradictory effective
setting rejection. The parser was also checked against retained real Kafka
startup/recovery logs; prior completed 32k and 50k results still validate. The
runner with timed-completion validation was uploaded after the external 50k
wrapper stopped. A fresh default-flush 32k pair is now running; its completed
result is still outstanding.

The working performance target is at least 80% of Kafka's sustainable throughput,
with no growing backlog or publish errors, and p99 end-to-end latency no more
than 1.25 times Kafka at the same offered load. Evaluate at least three runs per
system and report all runs, not only the best. Maximum-rate latency alone is not
a fair latency comparison when achieved rates differ.

Final acceptance also requires independent partition storage nodes, RF=3,
acknowledged-record survival under leader failure, and one-versus-multiple-node
scaling measurements. These gates are pending. Do not label a successful smoke
run or the existing custom Python measurements as performance parity.

Summarize completed result files with `python3 benchmarks/omb/summarize.py
/absolute/results/broker-max-1.json`. This reports observed rates, errors, backlog and
latencies; it deliberately does not declare parity without the configuration and
failure-test evidence. Inspect logs too: consumer exceptions are not represented
by the publish-error metric.

The included `kafka-rf1.properties` is the loopback-only OCI diagnostic reference
configuration. Change its data directory for another host before formatting a
fresh Kafka store. `kafka.sha512` pins the Apache 3.6.1 binary archive checksum
published at `https://archive.apache.org/dist/kafka/3.6.1/`.


## Automatic-rate diagnostic limitation

The pinned OMB `RateController` can enter an absorbing zero-rate state after a
backlog overshoot: its next expected count becomes zero, and multiplying zero by
the ramp-up factor cannot recover. The worker then clamps actual offered traffic
to one message/s. This was reproduced directly against the compiled upstream
class, without changing it. One broker max-rate run exhibited this behavior.
Keep the complete raw result, but do not call its average sustainable capacity or
use its early peak as a replacement score. The `10k` workload offers a fixed
10,000 messages/s for five minutes after one minute of warmup, avoiding automatic
rate control. The `20k` and `32k` workloads keep the same duration and client
settings at higher fixed offered rates. Fixed-rate sweeps, not a patched Kafka
client, establish capacity.

`profile-fetch.sql` isolates a second optimization candidate using a temporary
100,000-batch index. The current predicate filtered 99,999 earlier batches for a
tail read; the candidate seek used eight local buffer hits instead. This is query
plan evidence, not a broker speedup. The seek and commit-time notification hints are delivered in extension upgrade 0.1.1→0.1.2, with offset and
retention-boundary regression coverage. Earlier OMB runs use 0.1.1; their
results must not be attributed to 0.1.2.

[Initial measured results and limitations](results/2026-11-02/README.md) include
raw OMB output for Kafka, the original broker and the optimized broker. The
performance target is not yet met.


`profile-storage.sql` is a read-only, bounded post-run diagnostic for the physical
representation of the latest 100 batches in one partition. Kafka compression is
disabled in this workload, but PostgreSQL may independently compress its bytea
column through [TOAST](https://www.postgresql.org/docs/17/storage-toast.html).
Record compression and physical/logical byte sizes before attributing CPU cost to
this mechanism; the diagnostic does not prove its cost or justify a speedup claim.
Run it after the benchmark so it cannot compete with the measurement.

`profile-write.sql` is a separate post-run diagnostic for a disposable benchmark
database. It copies one stored batch 100 times through the public pgrx write API
into a temporary topic and rolls the transaction back. PostgreSQL's
[transaction-local function statistics](https://www.postgresql.org/docs/17/monitoring-stats.html#MONITORING-PG-STAT-USER-FUNCTIONS-VIEW)
separate the wrapper's self time from nested SQL dispatch time. This excludes
durable commit and network costs; it is not a throughput or acknowledgment
benchmark and must not run concurrently with OMB.

`profile-read.sql` performs ten bounded tail fetches using the SQL bulk-read API
after client traffic stops, reporting transaction-local function timings. It is
a read-only database diagnostic, not a network or gateway throughput comparison.

`profile-empty-fetch.sql` measures 100 database-only calls at the committed log
ends, where every partition returns an empty status. An iteration-dependent
argument prevents the planner from evaluating the table function only once.
Run it after traffic stops; it excludes gateway and network latency.

## Paired measurements on the isolated OCI host

`oci-pair.sh unique-label 10k|20k|32k` runs Kafka and the verified cached015 broker
sequentially on the existing dedicated four-core `/home/ubuntu` environment.
It is an environment-specific verification runner, not a provisioning script.
It requires the pinned Kafka distribution, built OMB checkout, broker image,
0.1.5 migrations and candidate binaries from the preceding verification. It
refuses changed candidate hashes, reused output/store names, active workloads
and insufficient disk space. A host lock prevents two copies of this runner.

Both systems receive CPUs 0–1 and 9 GiB total memory; the OMB worker uses CPUs
2–3. Each has a fresh store. Kafka retains the explicit per-message flush policy.
The runner records applied resource limits, binary hashes, Kafka properties,
client logs, workload files, results and post-restart broker storage checks under
`/home/ubuntu/results/omb-pair-<label>/`. Errors remain evidence even when the
load is unsustainable. An EXIT trap stops only this run's named services and
containers; disks are retained and the VM itself requires separate shutdown.
Do not run builds or profiling workloads during a comparison. A completed pair
is one trial; it does not replace the repetition or distributed acceptance gates.

`python3 benchmarks/omb/compare.py /path/to/pair [/path/to/repetition ...]`
compares complete paired evidence directories. It requires identical workloads,
client settings (except bootstrap endpoints), upstream revisions and complete
sample counts. It rejects repeated input paths and invalid latency values;
errors, warning counts, backlog trends and offered-rate fractions stay visible.
It reports every trial and its end-to-end p99 ratio, without declaring capacity
or parity. Applied resource limits, physical placement, store freshness,
durability and failure acceptance still require inspection of the run evidence.
Use separate invocations for different offered loads. Run its regression checks
with `python3 -m unittest discover -s benchmarks/omb/tests -v`.

`profile-toast.sql` compares temporary PGLZ-compressed and EXTERNAL (uncompressed)
storage for at most 100 existing partition batches, then rolls everything back.
Run it only after traffic stops. It reports insert/reconstruction time and storage
size, with identical-byte checks. It does not modify broker data or settings and
omits durable WAL/commit cost; cache and ordering effects remain. Use it to choose
experiments, not to predict a durable throughput gain. PostgreSQL's supported
[TOAST storage policies](https://www.postgresql.org/docs/17/storage-toast.html)
provide these options; the original benchmark PostgreSQL image lacks LZ4 support; the candidate
build adds it without changing the default compression policy.

`oci-compression.sh unique-label 10k|20k|32k` compares PGLZ and LZ4 on the same
verified LZ4-enabled candidate image, using fresh stores and identical CPU/memory
limits. It pins both runs to the resolved image ID, changes only the record-byte
column's compression method, and verifies actual stored compression plus durable
partition ranges after each database restart. PostgreSQL's default compression
is unchanged outside these disposable databases. Run only after the image build
and correctness checks succeed. Results and logs are preserved under
`/home/ubuntu/results/omb-compression-<label>/`; this runner retains its stores
and does not stop the VM. Its syntax and invalid-input guards have been checked;
the first complete cloud execution is recorded in
`results/2026-11-02/compression32k-1/`.

For the compression experiment, use `python3 benchmarks/omb/compare.py --reference
pglz --candidate lz4 /path/to/compression-pair`. The JSON explicitly names both
systems and reports candidate/reference latency ratios. Both wire producers keep
`compression.type=none`; this experiment changes PostgreSQL's internal record
storage. The comparator's six regression tests include ratio direction and label
validation, and its default Kafka/broker comparison remains supported.

The optional third argument `pipeline` selects the gateway comparison:
`oci-compression.sh unique-label 32k pipeline`. It runs `baseline` and `pipeline`
gateways sequentially on the same immutable runtime image, with LZ4 enabled for
both fresh stores. The pipeline executable must match the native-tested hash
embedded in the runner. Binary identities, applied resource limits, logs and
post-restart compression/offset checks are retained under
`/home/ubuntu/results/omb-pipeline-<label>/`. Compare completed results with
`compare.py --reference baseline --candidate pipeline /path/to/pair`. The first
pipeline pair completed with 20/19 ms end-to-end p99; this single trial does
not establish a repeatable gain (see `results/2026-11-03/README.md`).

`profile-database.sql` captures PostgreSQL 17 checkpoint, WAL, I/O and table-update
statistics after benchmark traffic stops. It is read-only and includes tracking
settings. Counters cover the entire fresh database lifecycle, including warmup
and restarts; they are not measurement-window deltas. Zero timing fields do not
establish zero I/O cost when timing collection is disabled. Use these diagnostics
to choose further measurements, not to infer the cause of a latency spike.

`profile-retention.sql` compares the current bounded retention-boundary aggregate
with an indexed first-live-row lookup, after traffic stops. Both queries are
read-only and bounded to 4,096 batches. It evaluates a possible no-op fast path;
it does not implement retention, cover all-expired pages, or prove any effect
on client-visible latency. Cache/order effects remain in this diagnostic.


The third argument `retention` compares SQL extension 0.1.5 and 0.1.6:
`oci-compression.sh unique-label 32k retention`. Both variants mount the same
verified pipeline gateway and use fresh LZ4 stores. Only the candidate receives
the SQL-only retention migration; its exact contents and checksum are retained.
Post-restart checks require the respective extension versions. Compare using
`compare.py --reference baseline --candidate retention /path/to/pair`.


The third argument `tail` compares SQL 0.1.6 and 0.1.7:
`oci-compression.sh unique-label 32k tail`. Both fresh stores receive the retention
migration and use the same verified pipeline gateway and LZ4 storage. Only the
candidate receives the caught-up binary-fetch early exit. Compare using
`compare.py --reference baseline --candidate tail /path/to/pair`.

`profile-fetch-tail.py` is a separate native SQL diagnostic. Supply a fresh,
empty disposable database with extension 0.1.6 via `--dsn` and a new JSON path
via `--output`. It inserts valid batch fixtures, measures 100 calls after ten
warmups for three consumption positions, then upgrades the same database to
0.1.7 and repeats. Raw samples and buffer plans are retained. It is sequential,
unreplicated and excludes Kafka clients; it cannot establish end-to-end gains.


The first completed `tail` pair is in `results/2026-11-03/tail32k-1/`.
Candidate end-to-end p99 was 12 ms at 32k messages/s, but the baseline suffered
large unexplained spikes (307.001 ms p99). Preserve those samples and do not
interpret the full ratio as a causal gain. Repeated fresh Kafka comparisons
are required before claiming parity.


### Repeated current-candidate comparisons

`oci-pair.sh unique-label 10k|20k|32k|50k [kafka-first|broker-first]` now compares
Kafka 3.6.1 against SQL 0.1.7, using the immutable LZ4 runtime image and verified
pipeline gateway. Older captured `runner.sh` artifacts preserve the prior
cached-0.1.5 experiments. The runner retains both stores and checks Kafka data-log
end offsets before/after clean restart as well as PostgreSQL's durable ranges.
`oci-repeat.sh unique-label 10k|20k|32k` runs three fresh pairs with alternating
order and stops on infrastructure failure; errors in completed load results
remain evidence rather than being filtered out. Do not edit an uploaded runner
during a sequence: each pair must match its initial captured source.

The dedicated VM's boot volume is now 160 GiB at 10 VPUs/GiB. Each new pair
captures `/home/ubuntu/omb-volume-profile.json` and guest disk geometry. Earlier
80 GiB results are historical; use fresh paired Kafka references because volume
size can affect I/O performance. Expansion followed Oracle's
[resize procedure](https://docs.oracle.com/en-us/iaas/Content/Block/Tasks/update-online-resize-block-boot-volume.htm)
and the Ubuntu guest grew its root filesystem on boot. Kafka's offset verifier
uses the pinned distribution's [GetOffsetShell](https://github.com/apache/kafka/blob/3.6.1/core/src/main/scala/kafka/tools/GetOffsetShell.scala)
with a benchmark-topic regex, excluding changing coordinator-internal offsets.


For the current Kafka/0.1.7 pairs, use
`python3 benchmarks/omb/compare.py --verify-environment /path/to/pair1 /path/to/pair2 /path/to/pair3`.
This additionally requires completion markers, the captured 2-CPU / 9-GiB
resource contract, SQL 0.1.7 durability/permissions/range checks, actual LZ4,
unchanged 16-partition Kafka endpoints across restart, and matching volume,
Java, host, image and SQL/runner provenance across trials. It rejects reused
captured store/container identities and mixed 80/160-GiB evidence. Run order
may differ. It checks captured artifacts rather than independently attesting
the live host or binaries, and it does not declare performance acceptance.
The original comparison mode remains available for historical artifacts that
predate these captures. All nine analysis regression tests pass.


Comparison summaries now also expose publish, end-to-end and producer-delay
p99.9, p99.99 and maximum values. Delay remains in microseconds; other latency
fields are milliseconds. Missing legacy fields are null, never zero, and invalid
supplied values are rejected. These fields supplement the declared p99 target:
meeting p99 does not demonstrate similar rarer-tail behavior. The completed
32k repetitions illustrate this distinction (broker p99.9 was about 2x Kafka
in two trials).

### Separate JVM pause diagnostic

`bash benchmarks/omb/oci-gc-profile.sh unique-label 32k kafka-first` prepares a
fresh paired run on the dedicated OCI host using the existing pair runner.
Run only after the active comparison finishes and its services stop. It retains
the existing 1 GiB client heap and adds GC/safepoint logs through `HEAP_OPTS`,
which the pinned upstream launcher accepts without client changes.
[Java 17 unified logging](https://docs.oracle.com/en/java/javase/17/docs/specs/man/java.html#enable-logging-with-the-jvm-unified-logging-framework)
provides timestamps, process-specific filenames and rotation. Four rotated files
of approximately 16 MiB each bound retained logging per JVM, in addition to the
current file. Preserve every log segment; rotation can discard early coverage.

The wrapper captures options, its source, a per-system log map and an explicit
`acceptance_run: false` marker. Logging adds overhead, so this run is diagnostic.
The comparison tool rejects mixing different instrumentation settings even without
`--verify-environment`; absent historical metadata means "not recorded", not
proof of no instrumentation. Wait for both the wrapper and pair completion
markers before collecting a diagnostic run. Compare pause timestamps with OMB
measurement windows before attributing stalls to the JVM; overlap alone does not
establish causation. Eleven analysis tests cover evidence validation and the
instrumentation separation. The wrapper passed shell syntax validation and its
logging flags passed a Java 17 preflight on the benchmark host. The first
separate 32k diagnostic pair is running; its client GC log is confirmed present.

The locally prepared gateway also accepts `PG_BROKER_TRACE_RETENTION=1` for
separate maintenance diagnostics. It emits Unix-millisecond start/end times,
monotonic elapsed microseconds, completed partition count and success for each
scan. This is disabled by default. Enabling it requires a separately identified
gateway build and an instrumentation marker; the existing GC wrapper deliberately
keeps its original pinned binary. Do not attribute a correlation to retention
until contemporaneous traces support it, or mix an instrumented run into the
uninstrumented repetitions.

After the diagnostic finishes, `gc_pauses.py --since <ISO-time-with-zone>
--until <ISO-time-with-zone> <all-log-segments-for-one-client>` extracts pauses
whose completion timestamp falls in the measured interval. Obtain the interval
from that client's OMB measurement-start and final measurement log lines, using
the recorded host timezone/date. Include every retained rotated segment and use
the wrapper's client map to select the correct JVM. GC and safepoint durations
overlap and must not be added. Empty results report null maximum durations;
observed timestamp ranges alone do not establish complete log coverage or prove
the absence of pauses. Fourteen analysis tests now pass, including fixtures from
the observed Java 17 log format, unit conversion and incomplete-segment rejection.

Add `--omb-json <completed-result> --omb-log <matching-client-log>` to align
pause counts/maxima with each measured window's publish/end-to-end p99 and
maximum latency and backlog. Bounds must exactly match the OMB measurement-start
marker and final window timestamp; sample counts must agree. These are coarse
log-time windows, not per-record timings. The fifteenth analysis test verifies
alignment and rejects mismatched bounds/counts.

### Server maintenance and slow-statement diagnostic

`bash benchmarks/omb/oci-server-profile.sh unique-label` runs only the broker
at 32k on a fresh store, using the separately pinned tracing gateway. It keeps
the existing durability and CPU/memory limits, captures client JVM pauses,
maintenance start/end timestamps, one-second host `vmstat`, and PostgreSQL
statements taking at least 10 ms. Parameter values are omitted. Checkpoint and
autovacuum logging plus I/O/WAL timing are enabled explicitly by
`server-profile.sql`; `server-profile-stats.sql` captures cumulative counters
before and after the workload, including warmup. These settings were verified
on a disposable PostgreSQL 17 instance with an actual slow statement.

This run has an `acceptance_run: false` marker and is not a Kafka comparison.
Timing/logging overhead may affect behavior. PostgreSQL container logs rotate at
approximately 16 MiB with four retained files; verify timestamp coverage before
interpreting absence of a slow statement. Counts and slow-statement durations do
not themselves distinguish CPU work from waiting. The runner captures source
hashes, actual settings, resource limits and post-restart storage checks. It
refuses existing output/store names or concurrent benchmark activity. Run it
under a bounded systemd unit as with other OCI diagnostics; completed stores
remain disposable, while raw evidence must be retained.

`oci-wal-init-pair.sh unique-label [on-first|off-first]` tests the WAL segment
initialization hypothesis using two fresh 32k broker stores with identical
instrumentation and the tracing binary. It calls
`oci-server-profile.sh label on|off crash`, explicitly reads back WAL settings,
keeps fsync/full-page writes/synchronous commits enabled, and records the chosen
mode in `storage-experiment.json`. After client traffic ends, each PostgreSQL
process is killed with SIGKILL and restarted. Partition endpoints must match
before/after, and the existing contiguous-log, permission and LZ4 checks must
pass. This covers process-crash recovery, not host/power loss or RF=3 durability.
The parent refuses runner changes between phases; retain both phases even if
the measured latency does not improve. Run under a 30-minute systemd bound.

`python3 benchmarks/omb/compare_wal_init.py <on-directory> <off-directory>`
checks completed diagnostic artifacts, matching client/deployment settings,
actual WAL/durability settings, distinct stores, unchanged partition endpoints,
crash redo logs and contiguous storage. It reports all latency metrics and slow
SQL statements from the measured interval. Client JVM timestamps supply the date
for OMB's time-only log lines, including midnight rollover. This is captured
evidence validation, not independent host attestation or performance acceptance.

For an uninstrumented Kafka comparison, `oci-pair.sh label rate order on|off`
and `oci-repeat.sh label rate on|off` now accept an explicit WAL initialization
mode, defaulting to `on`. They use the original pipeline gateway, read back
the actual WAL/durability settings, and retain requested/actual settings in the
evidence directory. Comparisons reject mixed modes even without environment
verification; older artifacts are labeled unrecorded rather than assigned an
invented default. Sixteen analysis tests pass, including weakened-durability
and mixed-mode rejection. The reversed-order diagnostic used its previously
captured scripts. The new runners
have now been uploaded after that diagnostic completed, and three alternating
Kafka/candidate pairs with `wal_init_zero=off` are running without profiling.

The analysis suite now has 21 passing tests. Standalone summaries reject
booleans, negative values and nonfinite required metrics as well as invalid
rare-tail values. Diagnostic WAL comparisons require one measured log window
per result sample, increasing timestamps across midnight, timezone-bearing
completion evidence, and matching crash-recovery/provenance artifacts. Tests
include truncated/out-of-order logs, changed clients, reused topics, lost offsets,
weakened durability and missing redo. Both completed WAL pairs and the completed
32k/50k comparisons also passed revalidation against their retained raw evidence.

### Next capacity and client-placement checks

`75k.yaml` offers 75,000 messages/s with the same record/client/topic settings
for 15 measured minutes after one minute of warmup. The local `run.sh` and
`oci-pair.sh` accept `75k`; the pair runner requires more than 95 GiB free before
starting and 90 samples per system. The active runner supports this workload; it has not been measured yet.
Do not replace captured scripts during a running pair.
The higher offered rate is an exploration point, not a throughput claim. If it
overloads either system, retain that result and bracket capacity with lower
fixed rates rather than selecting a peak or dropping failed windows.

A separate OCI client VM was provisioned using the supported
[instance launch CLI](https://docs.oracle.com/en-us/iaas/tools/oci-cli/latest/oci_cli_docs/cmdref/compute/instance/launch.html).
It has two Ampere CPUs, 12 GiB RAM and a 60 GiB boot disk in the benchmark VCN.
Resource IDs and bootstrap state are retained locally in
`.dev/oci-omb-client-resources.json`. It must build the same unmodified OMB
revision and use the same Java/client settings. Before traffic, capture both
hosts' resources and Java versions, use private broker endpoints, restrict
ingress to that client's private address, and validate metadata advertisement.
The private broker ports are now accessible only through the scoped host rule
described below. Separate-host comparisons use fresh Kafka references and remain
separate from the loopback series.
The second VM alone does not establish distributed broker storage or scaling.

`client-workload.sh` executes on that dedicated client under a systemd unit
named `omb-client-<label>.service`, with `AllowedCPUs=0,1`, `MemoryMax=2G` and
`RuntimeMaxSec=1600`. It explicitly selects the verified Java 17 runtime and
1-GiB heap, checks the clean pinned upstream checkout, prevents overlapping
workloads with a host lock, and captures scripts, payload checksum and actual
resource limits. It accepts only RFC1918 IPv4 broker endpoints on 9092/19092.
Its `--preflight` mode captures these checks without connecting to a broker and
writes a distinct marker, never a benchmark-completion marker.

The local controller uses the existing SSH key without copying it to either VM:

```sh
python3 benchmarks/omb/remote_client.py start /absolute/new-control-directory \
  --host CLIENT_SSH_IP --key /absolute/ssh-key \
  --known-hosts /absolute/verified-known-hosts --label unique-label \
  --rate smoke --bootstrap BROKER_PRIVATE_IP:9092 --preflight
python3 benchmarks/omb/remote_client.py collect /absolute/new-control-directory
```

The start command saves the exact service handle before sending the request.
On an observation failure, collect that same handle; do not start another run.
The service remains available after its process exits, so completed evidence
can be collected even after the controlling SSH session ends. Collection checks
the invocation identity and terminal process result, retains logs, and bounds
archive extraction to 128 MiB. Once broker networking and metadata routing are
verified, omit `--preflight` to execute the selected workload. Do not invoke it
against the currently running loopback experiment. Both the preflight and a
second collection of the same invocation succeeded on the real client VM;
evidence is retained under `results/2026-11-03/client-host-setup/preflight/`.
The initial controller preflight exposed transient-unit garbage collection;
`RemainAfterExit=yes` fixes that observation race. No Kafka traffic was generated
by these setup checks. The subsequent live paired run is recorded below.

The deployed server runner accepts `OMB_EXTERNAL_CLIENT=1` with an
explicit `OMB_SERVER_HOST` RFC1918 address. It binds and advertises that private
address, leaves the Kafka controller on loopback, and writes one request file
for each ready system. Each handoff is bounded to 1,600 seconds. The controller
can advance a requested phase without copying SSH keys to either VM:

```sh
python3 benchmarks/omb/handoff_client.py /absolute/new-phase-directory \
  --pair unique-pair-label --system kafka \
  --server-host SERVER_SSH_IP --server-private BROKER_PRIVATE_IP \
  --client-host CLIENT_SSH_IP --key /absolute/ssh-key \
  --server-known-hosts /absolute/server-known-hosts \
  --client-known-hosts /absolute/client-known-hosts
```

Repeat the exact command and directory to collect the same client invocation;
use a different phase directory for `--system broker` when its request exists.
Add `--wait` to poll that same phase every 30 seconds with a 30-minute observation
budget. It holds the local controller lock until delivery or exit. Individual SSH
operations retain their own timeout, so the observation budget is not a hard
wall-clock deadline. An observation error or timeout preserves the remote run
and its local handle; resume with the same directory. This option changes only
the local controller, not the measured workload or server configuration.
A missing request means that phase is not ready; inspect the original server
job rather than starting another pair. On success, the controller transfers a
checksum-verified archive to a staging directory and atomically publishes it.
The server checks endpoint, workload, revision and result length before running
its existing recovery checks. Lost transfer acknowledgments reuse the same
archive bytes and invocation. Local fault-injection tests cover that retry and
reject a changed request before touching the client.

New result directories explicitly record placement. Analysis rejects mixing
loopback and separate-host runs, preflight-only results, mismatched client
resources/Java/scripts/payloads, and inconsistent handoff copies. Historical
artifacts without placement metadata remain labeled unrecorded. All 38 analysis
tests pass, including flush-policy validation, bounded-wait handle reuse and
producer scheduling-delay growth with an empty consumer backlog. The external
handoff has now completed both phases of a live 32k pair, including restart checks.

Network inspection found that the dedicated VCN already permits intra-subnet
traffic, while the server's INPUT chain rejects new Kafka connections. After
all pair jobs stop, `oci-external-network.sh add CLIENT_PRIVATE_IP SERVER_PRIVATE_IP`
adds one temporary host rule scoped to those exact addresses and destination
ports 9092/19092. It checks the dedicated server hostname and assigned address,
takes the pair lock, and refuses active Docker/Kafka workloads. The corresponding
`remove` command deletes only its tagged rule. It neither exposes public Kafka
ports nor changes OCI's security list, and it does not persist across a host
reboot. Capture the resulting INPUT chain with the separate-host evidence.
The rule was applied after the loopback sequence finished. The completed external
32k pair verifies live metadata discovery, production and subscribed consumption
through the private endpoints. Network evidence is retained in
`results/2026-11-03/external-client-setup/`.

The first separate-host pair (`external32k-1`) completed at 22:11:18 UTC on
2026-11-03. Both systems sustained approximately 32k records/s across all 30
measurement samples with zero publish errors and no client warning/error matches.
Kafka/broker end-to-end p99 was 20/14 ms, p99.9 was 43/46 ms, and maximum was
154/131 ms. Final sampled backlog was 0/162 records. Both restart/storage checks
passed. The broker retained eight unclassified gateway deadline messages; those
are not silently excluded from the evidence. See `external32k-comparison.json`
and the checksummed raw directory under `results/2026-11-03/`.

This is one RF=1 trial with Kafka's per-append flush policy. It is not evidence of
default-Kafka parity, maximum sustainable capacity or distributed scaling. The
subsequent separate-host 50k pair uses fresh stores and a 15-minute measurement
per system. Its broker phase completed all 90 samples at 50,014.86 published
and 50,014.80 consumed records/s, zero publish errors, 26 ms end-to-end p99,
117 ms p99.9 and 336.001 ms maximum. Final/peak sampled backlog was 307/499.
The post-restart storage check passed. Interim artifacts are retained in
`results/2026-11-03/external50k-broker-interim/`; Kafka warmup began at 22:31:53 UTC
and completed, but the wrapper subsequently rejected its 89-sample output;
see the failed-wrapper record below. That pair retained
its original SQL 0.1.7, gateway binary and runner; experimental SQL 0.1.8 and the
new default-flush runner option have not been deployed to it.

The local analyzer now retains first/last/maximum average producer scheduling
delay and its linear trend per measurement sample. A zero consumer backlog does
not mean the offered workload was sustained: records can be delayed before being
published. Missing historical window data stays explicitly unknown. This analyzer
change is local only; the running client/server and the prepared background-flush
archive retain their captured script versions to preserve workload provenance.

The pinned upstream `WorkerStats.recordProducerSuccess` clamps publish latency
and producer scheduling delay to 60 seconds before histogram recording:
[revision-pinned source](https://github.com/openmessaging/benchmark/blob/5b1fa70951a323da26bd587174b58bb2c65b0b5c/benchmark-framework/src/main/java/io/openmessaging/benchmark/worker/WorkerStats.java).
HdrHistogram bucket rounding can report slightly above that value. The local
summary flags reported quantiles at this cap only when the result's revision
sidecar matches the pinned source. Such values may be censored; a plateau at the
cap cannot establish stable scheduling delay. The flag does not infer the real
uncensored delay and does not apply the producer cap to end-to-end latency.

#### External 50k wrapper failure and timed-completion correction

The first external 50k client pair finished its measurements, but the server
wrapper failed its exact-90-sample assertion before running Kafka restart
checks. Raw evidence and the failure remain in
`results/2026-11-03/external50k-failed-wrapper/`; this is not a successful
recovery-verified pair. Kafka produced 89 windows over 900.13 seconds and the
broker produced 90 over 903.31 seconds. The upstream loop sleeps ten seconds,
collects statistics, then checks elapsed time; it does not promise exactly six
windows per minute.

The corrected `measurement.py` verifies the requested duration using measurement
start/sample/aggregate/result-writing events, matches logged samples to result
arrays, handles midnight, and rejects truncated runs, duplicate starts and clock
reversal. The live runner requires this evidence. The analyzer explicitly labels
legacy nominal-count-only fixtures as timing-unverified. This changes only the
wrapper and analysis, not upstream Kafka clients. All 38 analysis tests pass,
and both actual external 50k client logs pass timed-completion validation.

The failed-wrapper measurements averaged 45,665 Kafka versus 50,015 broker
records/s, with end-to-end p99 of 1201.007 versus 26 ms. Kafka scheduling delay
reached the upstream 60-second cap. These unequal achieved loads and the missing
Kafka restart check preclude a full comparison acceptance claim. The run remains
visible rather than being discarded or relabeled as successful.

The fresh default-flush trial `017-background32k-1` started at approximately
22:53 UTC on 2026-11-03. The Kafka client uses the same separate host, unchanged
upstream revision and client scripts. The server runner now uses timed-completion
validation and explicitly records background flushing. PostgreSQL retains SQL
0.1.7, the original pipeline gateway and all durability settings. Local controller
state is in `.dev/omb-active-background32k017.json`; no result is claimed yet.
The older verified external32k Kafka scratch store was reclaimed under the idle
lock after matching its local/remote result hashes, leaving about40GiB free; its
full raw evidence remains retained. The failed50k scratch store was not removed.

The default-flush pair completed successfully at approximately 23:09 UTC. Its
full capture and verified comparison are `results/2026-11-03/background32k-1/`
and `background32k-comparison.json`. Kafka/broker published 32,028.64/32,027.87
records/s, with zero publish errors and zero final sampled backlog. End-to-end
p99 was 1192.007/14 ms, p99.9 was 2098.015/38 ms, and maximum was 2345.007/136 ms.
Both restart checks and requested/effective Kafka flush settings passed.

Read-only OCI monitoring evidence in `results/2026-11-03/volume-metrics/` records
1085 throttled disk operations during four interior Kafka default-flush minutes
(22:55–22:58 UTC), versus zero in the checked interior minutes of the earlier
50k broker and Kafka phases. These minute aggregates do not identify the cause
of each latency spike. The 160 GiB/10 VPU profile is preserved as a disk-constrained
reference; a larger profile must be evaluated with fresh runs of both systems,
not pooled with these trials. No broad parity or distributed-storage acceptance
is inferred from this single comparison.

After that pair completed and all benchmark processes stopped, the dedicated
boot volume was expanded from 160 to 320 GiB while retaining 10 VPUs/GB. The
ext4 partition/filesystem expansion passed; about 184 GiB was free afterward.
Cloud before/after records, partition tables, filesystem output and checksums
are in `results/2026-11-03/volume320-setup/`. Both storage capacity and provisioned
performance change with volume size, so old and new profiles cannot be pooled.

The first 75k offered-load pair on the new profile, `017-wide75k-1`, completed
at 23:49:56 UTC with successful restart checks and captured-environment validation.
It used Kafka default flushing, the separate client, original pipeline binary
and SQL 0.1.7, with one minute of warmup and fifteen minutes of measurement per
phase. Kafka/broker averaged 75,017/75,162 published records/s, with end-to-end
p99 of 615/712 ms and p99.9 of 1,073/843 ms. Final backlogs were 289/0 and both
reported zero publish errors, but broker offset commits failed extensively.
Kafka had zero logged client error events. Raw evidence and the updated analysis
are in `results/2026-11-03/wide75k-1/` and
`results/2026-11-03/wide75k-comparison-with-client-events.json`. This is one
storage-constrained RF=1 comparison, not sustainable-capacity or parity acceptance.

The local runner also accepts `OMB_GATEWAY_PATH` and `OMB_GATEWAY_SHA256` for a
separately built candidate. Paths must match
`/home/ubuntu/candidate-<label>/pg-broker-gateway`; the SHA-256 must match the
actual file before any workload starts. Defaults retain the original pipeline
binary. New runs preserve `gateway-sha256.txt` alongside the successful checksum
check, and environment verification includes the digest when comparing trials.
Historical captures without that file remain explicitly without this extra
digest field. Do not replace a binary or upload runner changes during a trial.

Offset-commit batching has passed native correctness tests and one complete OCI
comparison. Its offline ARM64 release build completed in 43.63 seconds,
with gateway SHA-256
`09f7592c2abb6e98c50fc2d3dad3dded9732b243a216fe80e7b4d528e6ea6cda`.
Build logs and source hashes are in `results/2026-11-03/commitbatch-build/`.
The candidate's broker phase completed around 00:09 UTC on November 4: 90
measurement windows over 905.788 seconds, 75,054 published records/s, zero
publish errors and zero logged client/offset-commit failures. End-to-end p99
was 677 ms versus the original binary's 712 ms, while p99.9 was 966 ms versus
843 ms. Final backlog was 253. The producer caught up by the final window, but
intermediate scheduling delays reached seconds. The full pair completed at 00:26:53 UTC with both restart checks and environment
validation. Kafka published 75,044 records/s with zero logged client failures;
its end-to-end p99/p99.9 were 725/1,104 ms, with final backlog zero. The broker
had scheduling-delay p99 of 3.018 seconds versus Kafka's 0.334 seconds. Full
evidence is in `results/2026-11-03/commitbatch75k-1/` and the adjacent
`commitbatch75k-comparison.json`; the directory date follows the launch date. Completed
client evidence and a narrowly scoped before/after report are saved in
`results/2026-11-04/commitbatch75k-broker-interim/`. This single observation
supports eliminating the observed commit timeouts in this run, not a general
latency, capacity, durability or replication-parity claim.
Its source archive is pinned in
`.dev/commitbatch017-source-hashes.json`. The controller
`.dev/build-commitbatch-after-wide.py` waits for the exact original 75k pair to
finish successfully and be collected, then acquires the host's benchmark lock,
checks that workloads are stopped, and starts an offline build in the cached
ARM64 builder. It writes a durable build handle before launching; an observation
failure must be resolved against that handle rather than relaunching the build.
This controller only builds; it does not launch another benchmark.

The separate `.dev/run-commitbatch75-after-build.py` controller verified
that exact build, recorded its digest and logs, and started one broker-first
75k pair on the unchanged 320 GiB/default-flush profile. It refuses to launch
without more than 95 GiB free, never reclaims data automatically, and owns both
client handoffs and collection. Its durable benchmark handle is
`.dev/omb-active-commitbatch75k017.json`; inspect that handle after an observation
failure instead of starting another workload.
The initial orchestrator encountered a local parent/child lock-name collision
after starting the server, before launching any client. Separate lock names
fixed the issue; the handoff resumed against the same server invocation, without
restarting the pair. The separate sequence and collector completed successfully.

Comparison reports now distinguish timestamped client warning/error events from
stack-trace text and report offset-commit failure events explicitly. Counts cover
the entire retained client log, including warmup/shutdown, and are neither
lost-record counts nor measurement-only rates. The original 75k broker log has
32,987 such commit-failure events despite zero reported publish errors. This
disqualifies a reliable-capacity claim based only on its average throughput.

Read-only OCI metrics for the expanded volume report 19,200 provisioned IOPS
and a guaranteed-throughput metric of 150 (twice the old profile). However,
`results/2026-11-03/volume320-metrics-interim/` records zero throttled I/Os in
the fourteen checked broker minutes (23:16–23:29 UTC), versus 3,512 in eight
checked Kafka minutes (23:34–23:41 UTC). The Kafka phase was still running;
missing points are not treated as zero. Minute aggregates cannot attribute each
latency spike, but the reference still encounters volume throttling. The queued
candidate run retains this profile to isolate the code change; broader parity
requires additional runs with storage headroom, not a claim based on these tails.

After the candidate pair completed and its evidence was collected,
`.dev/expand640-after-commitbatch.py` expanded the dedicated volume from 320 to
640 GiB while retaining 10 VPUs/GB, finishing at 00:30:23 UTC on November 4. It checks the exact parent
invocation, holds the host benchmark lock continuously across cloud/guest changes,
uses an ETag-conditional cloud update, and verifies both partition and filesystem
growth before updating the profile marker. The durable resize handle is
`.dev/omb-volume640-controller.json`; an uncertain resize outcome must be inspected
against the same volume, never blindly replayed. Oracle's
[performance specification](https://docs.oracle.com/en-us/iaas/Content/Block/Concepts/blockvolumeperformance.htm)
motivates this higher-throughput profile, but actual throttling must be measured.

`.dev/run-headroom75-after-resize.py` started one fresh Kafka-first 75k pair
after confirmed resize completion, with the same pinned batching gateway and
workload. It checks the exact binary/runner hashes, guest disk size, profile and
free space before launch. Its handle is `.dev/omb-active-headroom75k017.json`.
The 640 GiB results will be kept separate from all earlier profiles; the new
comparison is running, not complete. Initial OCI metrics report 25,000 guaranteed
IOPS and a guaranteed-throughput value of 300, in
`results/2026-11-04/volume640-metrics-initial/`. These initial points verify the
allocation, not under-load performance or absence of throttling.

The first resize preflight stopped without changes because its broad `pgrep -f`
matched its own command. Read-only checks confirmed the original 320 GiB disk,
profile and idle host before replacing it with an exact Java-process check.
The waiting benchmark launcher was stopped before launch and restarted with the
same correction. The failed preflight is retained in
`results/2026-11-04/volume640-preflight-failed/`; the completed conditional update,
guest partition/filesystem checks and profiles are in `volume640-setup/` alongside
it. No benchmark workload was restarted because of this preflight failure.

### Gateway diagnostic classification and candidate storage metrics

`compare.py` now reports retained gateway diagnostics separately from client
exceptions. Exact deadline contexts distinguish header reads, payload reads,
request handling, response writes and database work. Legacy deadline messages
remain unclassified; unknown diagnostic lines remain counted. A missing log is
`recorded: false`, not a zero-error result. Header-read deadlines cannot distinguish
idle sockets from incomplete requests and are not automatically exonerated.

The completed batching candidate has 18 header-read deadlines, no other recorded
gateway diagnostics, and no client warning/error events. Its original raw checksums
still match. The additive derived report is
`results/2026-11-03/commitbatch75k-comparison-with-gateway-events.json`;
the original report remains intact. Older runs' context-free deadline messages
remain unclassified, including all 18 in the preceding 75k run.

Historical OCI metrics for the completed candidate pair are retained in
`results/2026-11-04/commitbatch75k-volume-metrics/`. Across 13 conservative interior
minute points per system, the broker has zero throttled I/Os; Kafka has 6,197,
with a nonzero count in all 13 points. Both windows have complete point coverage.
The provisioned metrics are 19,200 IOPS and throughput 150. This confirms that
the reference encountered storage throttling; minute aggregates do not identify
the cause of each latency stall or explain the broker's scheduling delay.
The active 640 GiB pair tests a different storage profile and cannot be pooled
with these results. No gateway binary, SQL, client or runner changed during it.

The first live 640 GiB storage capture, in
`results/2026-11-04/headroom75k-metrics-interim1/`, contains four returned minute
points (00:32–00:35 UTC): throttled I/O counts 0, 0, 60 and 167. The allocation
remains 25,000 IOPS/throughput metric 300. The larger volume has therefore not
eliminated throttling in this partial observation. It is not a completed trial
or proof of the bottleneck behind each stall; the running pair continues with
unchanged settings. Oracle's [metric reference](https://docs.oracle.com/en-us/iaas/Content/Block/References/volumemetrics-reference.htm)
defines throttled I/Os as operations, not dropped Kafka records.

A local Produce frame-validation correction was tested while this comparison ran.
It is not included in the immutable benchmark binary. New builds must record the
new source/binary identity rather than attributing these live measurements to it.
The gateway analyzer also recognizes the exact request-failure context wrapper
around database deadline messages; arbitrary unknown contexts stay unclassified.
All 45 analyzer tests continue to pass.

### Completed 640 GiB Kafka phase (2026-11-04 UTC)

The first Kafka client phase finished 90 measurement windows over 904.840 seconds:
75,031 records/s published and consumed, zero publish errors, zero final backlog,
and no logged client warning/error/commit-failure events. End-to-end p99 was
208 ms, p99.9 490 ms and maximum 745 ms; scheduling-delay p99 was 351 microseconds.
This is materially lower than the completed 320 GiB candidate pair's Kafka
725 ms p99, but those storage profiles cannot be pooled or treated as repeated
trials. The client evidence is in `results/2026-11-04/headroom75k-kafka-interim/`.
The broker phase is now running through the same controller; full paired
recovery/environment validation and repeatability remain pending.

Historical read/write operation and throughput metrics for this phase are in
`results/2026-11-04/headroom75k-kafka-volume-metrics/`. The conservative interior
window has 12 of 13 expected minute points, with 00:36 UTC missing. Observed
throttled I/Os sum to 227; this is not a complete-window total and the missing
minute is not counted as zero. Minute aggregates cannot identify individual
stall causes.

### Completed 640 GiB comparison and Produce batching candidate

`017-headroom75k-1` completed with both restart checks and full environment
validation on November 4 at 01:06 UTC. Kafka/broker published approximately
75,031/75,122 records/s, with zero publish errors, zero final backlog and no client
warning/error/offset-commit failures. End-to-end p99 was 208/659 ms (broker/Kafka
ratio 3.168); p99.9 was 490/809 ms. Broker scheduling-delay p99 was 1.382 seconds
versus Kafka's 351 microseconds. All 18 gateway diagnostics were header-read
deadlines, not recorded database deadlines. This trial misses the working latency
target; average throughput alone does not establish capacity or parity. Complete
evidence is in `results/2026-11-04/headroom75k-1/` and
`headroom75k-comparison.json` alongside it.

A new local gateway candidate batches binary appends for all partitions in one
Produce request into one confirmed PostgreSQL transaction. It preserves duplicate
partition order, takes locks in sorted topic/partition order, and bounds expanded
inputs. Per-partition domain errors remain independent; internal database failure
rolls back the entire request. The existing per-partition fallback remains for
older extensions. This targets commit overhead; it has no measured OCI result yet.

The offline build controller `.dev/build-producebatch-after-headroom.py` requires
the full local test result, exact source hashes and the completed/collected prior
pair before acquiring the idle host lock. The subsequent
`.dev/run-producebatch75-after-build.py` owns a fresh broker-first 75k comparison
on the unchanged 640 GiB/10 VPU profile, runner and SQL 0.1.7. Durable handles are
`.dev/omb-active-producebatch017-build.json` and
`.dev/omb-active-producebatch75k017.json`. Inspect these original invocations after
an observation failure; never launch a duplicate workload. Snapshot experiments
and their opt-in SQL are not deployed by this gateway-only build.

The completed 640 GiB broker phase's OCI metrics are retained in
`results/2026-11-04/headroom75k-broker-volume-metrics/`. All 13 checked interior
minutes (00:51–01:03 UTC) are present and report zero throttled I/Os. This does
not establish the cause of individual stalls, but the observed broker latency
gap cannot be attributed to reported volume throttling in those minutes.
The Produce batching candidate passed the full native suite: 100 passed, four
opt-in crash cases skipped. Its first run's lock-observer failure and corrected
rerun are retained in `tests/results/2026-11-04-produce-batching/` at repository root.

The gated ARM64 build completed successfully in 43.62 seconds. Its binary SHA-256
is `0e0b5024eb643859f18b23a111870a45f28a2e65e522505ddeac8592bddb41fc`;
source hashes, build logs and terminal status are in
`results/2026-11-04/producebatch-build/`. The fresh broker-first comparison
`017-producebatch75k-1` is now running. This launch is not evidence of a performance
gain; the completed 659/208 ms baseline remains the measured latency gap.

### Produce batching: completed broker phase, paired result pending (2026-11-04)

The first 75k Produce-batching candidate broker phase completed 90 windows over
903.662 seconds on the 640 GiB/10 VPU profile. Mean publish/consume rates were
75,029/75,030 records/s, end-to-end p99/p999 were 148/438 ms, and producer scheduling
p99 was 537 microseconds. Final backlog was zero (maximum 3,795); publish errors
and logged client warning/error/offset-commit failures were all zero. The prior
640 GiB candidate's broker p99 was 659 ms. This is a single-run comparison across
candidate changes and run order, not proof of causality or parity.

Completed client evidence is preserved in
`results/2026-11-04/producebatch75k-broker-interim/`. The matching Kafka phase,
full pair recovery/environment verification and repetition remain pending.
The existing controller owns the handoff; two further alternating-order trials
are gated on the first complete pair's results and disk/artifact preflight.
The repetition plan is retained in
`results/2026-11-04/producebatch-repetition-plan/`.

### Produce batching: first complete matched 75k pair (2026-11-04)

The first Produce-batching pair completed with both clean-restart checks and full
captured-environment verification. Kafka/broker publish rates were 75,031/75,029
records/s; end-to-end p99 was 188/148 ms (broker/reference ratio 0.787), and p999
was 486/438 ms. Scheduling-delay p99 was 369/537 microseconds. Both had zero
publish errors and zero logged client warning/error/offset-commit failures.
Final backlog was 250/0. Both completed 90 windows (904.305/903.662 seconds).
The gateway logged 18 header deadlines and no other classified deadline types;
header deadlines may reflect idle or incomplete requests, with cause unknown.

The complete evidence supersedes the broker-only interim view:
`results/2026-11-04/producebatch75k-1/` and
`results/2026-11-04/producebatch75k-comparison.json`.
This meets the working latency target in one RF1 fixed-load trial. It does not
establish maximum sustainable capacity, repeatability, matching acknowledgment
durability, or RF3/distributed scaling. Two additional alternating-order trials
are controlled by the retained repetition plan; all outcomes will be preserved.

### Prepared 100k capacity step (not deployed)

`100k.yaml` keeps the 75k workload's 1 KiB records, 16 partitions, one-minute
warmup and 15-minute measurement, raising only the offered rate to 100,000/s.
Local runners and the external-client handoff accept this rate; the pair runner
requires more than 125 GiB free and enforces the full measurement duration.
Shell syntax and all 45 analysis/handoff tests pass. Preparation evidence is in
`results/2026-11-04/100k-preparation/`.

These scripts have not been uploaded to either benchmark host. The live 75k
repetitions retain their previously captured runner/client hashes. Deploy this
capacity step only after that sequence is terminal and collected, with fresh
stores and explicit disk preflight. No 100k result or sustainable-capacity claim
exists yet; use paired measurements and retain failures rather than selecting
only successful rates.

### Second matched Produce-batching pair complete (2026-11-04)

The Kafka-first second pair passed both clean-restart checks and full environment
validation. Cross-trial validation also confirmed matching captured artifacts,
resource settings and independent stores. Kafka/broker rates were 75,025/75,027
records/s, end-to-end p99 was 206/96 ms, p999 was 510/305 ms, and scheduling-delay
p99 was 368/421 microseconds. Both had zero publish errors and zero logged client
warnings/errors/offset-commit failures. Final backlog was 44/554; broker backlog
trend was -0.060 messages/s, with a 3,960-record peak. Neither final backlog nor
a fitted trend alone establishes long-run capacity. Both have 90 windows covering
904.695/903.528 seconds. The gateway retained 18 header-deadline events.

The two p99 ratios are 0.787 and 0.466. Full evidence is in
`results/2026-11-04/producebatch75k-2/`; the combined report is
`results/2026-11-04/producebatch75k-through-trial-2.json`.
Broker storage metrics contain all 13 conservative interior minute points with
zero throttled IO. Kafka has 11 of 13 points with 238 observed throttled IOs;
01:59 and 02:00 UTC are missing, not zero. These minute aggregates cannot identify
individual-stall causality. Raw metrics and checksums are retained separately.

Trial three failed during artifact handoff after its broker measurement; its
Kafka measurement and restart checks did not run. Evidence is retained under
`results/2026-11-04/producebatch75k-3-failed/`. A fresh fourth attempt runs under
the cloud controller, with unchanged benchmark artifacts. No historical stores
were deleted. The prepared 100k step needs fresh disk preflight after collection.

### Cloud controller and next capacity gate

The cloud controller runs handoffs on spare server CPUs 2–3, independently of
desktop availability. Its durable state identifies the exact systemd invocation;
an existing control directory refuses another launch. Observe the recorded job
after a timeout instead of restarting it. The current fourth attempt still uses
the captured original controller from `results/2026-11-04/cloud-controller-setup/`.

The local controller now accepts `--rate 100k`, an explicit reviewed
`--runner-sha256`, and `--volume-size-gib`. It rejects the old 75k-only runner
digest for 100k, checks more than 125 GiB free, and requires the recorded storage
profile to match. All 51 harness tests pass, including rate forwarding, storage
preflight and prevention of duplicate launches. This preparation is not deployed
and does not establish capacity.

Retained stores mean a larger storage allocation will be needed before the next
series. Any resize must occur after the current pair is terminal and collected,
under the server's idle lock, with fresh matching Kafka/broker references afterward.
At 10 VPU/GB, increasing size also changes provisioned throughput until its cap;
it is not merely a free-space change. Oracle documents 60 IOPS/GB capped at
25,000, and 480 KB/s/GB capped at 480 MB/s for the Balanced tier
([primary performance reference](https://docs.oracle.com/en-us/iaas/Content/Block/Concepts/blockvolumeperformance.htm)).
Capture the actual cloud/guest settings and performance metrics; never pool
results across different storage profiles.

### Completed 100k overload probe (2026-11-04 UTC)

On the new 1024 GiB / 10 VPU profile, one complete Kafka-first pair produced
100,061.95 messages/s for Kafka and 91,378.93 for the broker. End-to-end p99 was
363.001 versus 606.003 ms (1.6694×). Both ran at least 900 measured seconds,
reported zero publish errors and passed the retained restart checks. Broker
scheduling delay reached the upstream 60-second histogram cap: **100k is not a
sustainable accepted load**, regardless of the small consumer backlog. This
result remains separate from the three completed 640 GiB / 75k pairs.

All 13 interior broker storage-metric minutes show zero throttled I/O. Kafka has
13 of 14 points with zero observed throttling; the missing minute is not zero.
These aggregates do not locate the bottleneck. The next step is separate overload
profiling and fresh matched intermediate-load tests, followed by repeated
capacity and RF3/scaling acceptance. See
[the complete comparison](results/2026-11-04/capacity100k-1-comparison.json) and
[verification notes](../../docs/verification.md) for diagnostics and limitations.

### Broker-only overload diagnosis

`cloud-controller.py --diagnostic-broker-only --rate 100k --order broker-first`
uses the usual explicit runner hash and volume-size arguments and a fresh label.
The mode runs only the broker phase, keeps the ordinary client/workload and
gateway binary, and captures two-second process CPU/I/O, container counters and
aggregated PostgreSQL waits. It does not capture query text or credentials.
Sampling has a 550-sample cap and each database query a one-second statement
timeout. Containers stop only through the normal workload completion/cleanup
path; the observer never starts or stops them.

The output is explicitly marked `acceptance_run: false`. Its report validates
the measurement and sampling but never calls the paired comparison path.
Missing samples are errors, not zero load. `summarize-server.py` accepts explicit
timezone-qualified `--start` and `--end` boundaries, excluding warmup when chosen
from the client log. CPU units are average cores; wait counts are sampled active
backend observations, not wait durations. I/O timing remains at its existing
setting, which is captured alongside statistics; disabled timing cannot establish
I/O latency. This diagnosis should guide a subsequent controlled change and fresh
uninstrumented matched trials.

Pass `--stats-before` and `--stats-after` to `summarize-server.py` to include the
database lifetime counters. These include warmup and the sampler's own read-only
transactions, so commit counts must not be presented as Produce request counts.
The analyzer rejects counter resets and only calculates mean WAL write/sync
latency when both snapshots record timing enabled. PostgreSQL's
[statistics documentation](https://www.postgresql.org/docs/17/monitoring-stats.html)
also explains counter reporting lag; the
[runtime settings](https://www.postgresql.org/docs/17/runtime-config-statistics.html)
document timing overhead and `track_wal_io_timing`. These diagnostics do not
change acknowledgment durability.

For a separate timing experiment, add `--diagnostic-timing` to the broker-only
controller command. The runner verifies `track_io_timing=on`,
`track_wal_io_timing=on` and `track_functions=all` before starting the gateway,
records a three-second `pg_test_timing` result, and captures function statistics
in both snapshots. It leaves `fsync`, `full_page_writes` and `synchronous_commit`
enabled. Function total and self times are reported separately; nested totals
must not be summed. This instrumentation can affect results, so it is never an
acceptance configuration. The sampler also captures host `/proc/stat` for later
inspection of per-CPU utilization.

A prepared diagnostic-only group-commit variant adds
`--diagnostic-commit-delay-us 500`, requiring `--diagnostic-timing`. It sets
`commit_delay=500` microseconds and `commit_siblings=1` before gateway startup,
captures and checks both settings, and keeps synchronous commit and fsync enabled.
The default remains zero delay. This is an experiment, not a recommended or
performance-verified default: the added delay can help concurrent flush sharing
or hurt latency, as described in the
[PostgreSQL WAL settings](https://www.postgresql.org/docs/17/runtime-config-wal.html).
Use a fresh store after the unchanged baseline is complete; never alter an active
measurement. Candidate improvements still require fresh uninstrumented matched
Kafka trials and restart/failure verification.

Once both instrumented runs are complete, `compare_commit_delay.py` validates
their reviewed runner/sampler hashes, restart checks, sampling, timing settings,
resource limits and common client/binary/SQL contract. It preserves the full
throughput, scheduling-delay, error and tail-latency summaries alongside ratios.
The original zero-delay baseline omitted commit settings from its two statistics
snapshots, so comparison requires its separately captured read-only settings file
via `--observed-baseline-settings`; it does not silently assume defaults. The
500 µs candidate must contain the expected settings in both snapshots. This
single instrumented comparison is not an acceptance result.


### Intermediate capacity test preparation (2026-11-04 UTC)

`85k.yaml` preserves the 16-partition, 1 KiB, one-producer/one-consumer
configuration and one-minute warmup plus fifteen-minute measurement, offering
85,000 messages/s. The local pair, client and handoff tools now accept this rate.
The cloud controller requires an explicit reviewed runner digest and at least
125 GiB free, as for the 100k test. Its default remains 75k; broker-only timing
diagnosis remains restricted to 100k. All 94 harness tests and shell syntax checks
pass; frozen evidence is in `tests/results/2026-11-04-85k-preparation/`.
This preparation has not been deployed and makes no sustainable-capacity claim.
The live response-framing diagnostic uses its previously frozen harness.


The local controller also supports `--candidate-pair` with
`--partition-guard-candidate`, an explicit `--bulk-commit-binary`, its reviewed
`--bulk-commit-sha256`, and a reviewed `--runner-sha256`. This deliberately runs
both systems on SQL 0.1.18 without broker-only diagnosis, timing instrumentation
or experimental commit delay. The runner independently validates these mode
constraints. Defaults remain the existing SQL 0.1.7 pair.

For offline comparison use `compare.py --verify-environment
--expected-sql-version 0.1.18 --expected-gateway-sha256 <reviewed-digest>`.
The expected revision and digest are caller inputs, not inferred from captured
output. Verification checks the complete migration list against the reviewed
SQL 0.1.18 hashes in `sql_contract.py`, as well as existing resource, durability,
restart, client and workload checks. A self-consistent but changed migration is
rejected. Deploy `sql_contract.py` alongside `environment.py` and `compare.py`.
All 99 harness tests pass, including actual shell validation and negative
artifact checks. Frozen preparation is in
`tests/results/2026-11-04-candidate-pair-preparation/`; it is not yet deployed.
Three completed pairs at an offered rate still do not establish Kafka's maximum
sustainable capacity or RF3/scaling acceptance.

### Recorded-interval reporting (2026-11-04 UTC)

The local comparison now additionally integrates publish, consume and publish-error
rates using each observed log interval. Backlog regression uses elapsed seconds
instead of assuming ten-second windows. Legacy arithmetic means remain visible;
logs without timing evidence receive no time-weighted estimates and explicitly
label their backlog time basis as nominal. Log timestamps have millisecond
precision and reporting overhead, so these estimates are not exact message
counters and do not independently prove sustainable capacity.

The 101-test harness suite passes, including unequal intervals spanning midnight,
missing timing, inconsistent coverage and invalid sample rejection. The historical
supplement `results/2026-11-04/timed-rates-audit.json` leaves original reports intact.
For the query/reset diagnostic, the weighted publish rate is 92,685.39 msg/s versus
the arithmetic mean of 92,682.93; the correction does not change its failure to
sustain 100k. No live runner, client workload or deployed artifact was changed.
Evidence: `tests/results/2026-11-04-timed-rates/tests.log` in the repository root.


### First complete 85k candidate pair (2026-11-04 UTC)

The first uninstrumented matched SQL18 query/reset pair completed under invocation
`e3e4046642094bc19b7be1ccfe9f093e`, Kafka first, with the same 1024 GiB / 10 VPU
profile and pinned gateway digest `9130352c6bdd0a78321ce4bbea5ea4badb9714f23222f2abfab86b433c6bf43e`.
Kafka measured 905.427 seconds and the broker 904.085, each with 90 windows.
Kafka/broker arithmetic mean publish rates were 85,036.41 / 85,049.41 msg/s;
p99 end-to-end latency was 234 / 184 ms (broker/reference ratio 0.7863).
Both had zero publish errors and neither producer histogram reached the 60s cap.
Broker scheduling-delay p99 was 735 microseconds, versus Kafka's 704.

Environment, full-duration and post-restart checks passed. The entire broker
client log still contains one invalid-fetch INFO event after LeaveGroup during
shutdown; neither client logged warnings, errors or offset-commit failures.
The gateway recorded 18 header-read deadlines of unclassified cause, and no
request, response-write, database or payload-read deadlines. These observations
remain in the report; they are not silently treated as proof of no failures.

This meets the working latency target for one fixed-load trial, not repeatability,
maximum sustainable capacity, matching acknowledgment durability, independent-host
RF3 or scaling acceptance. The reverse-order trial uses the identical deployed
artifacts and fresh stores. Raw evidence and reports are in
`results/2026-11-04/capacity85k-1/`, `capacity85k-1-comparison.json` and the supplemental
`capacity85k-1-timed-comparison.json`. The original report is preserved unchanged.


### Prepared 110k capacity step (not deployed)

`110k.yaml` and the local controller/client handoff now support 110,000 messages
per second with the same 16 partitions, 1 KiB records, one producer and consumer,
one-minute warmup and full 15-minute measurement as the 100k workload. The
controller requires an explicit reviewed runner digest; controller and runner
both require more than 150 GiB free. SQL18 candidate comparisons retain the
reviewed binary contract and reject diagnostic instrumentation. All 105 harness
tests and shell syntax checks pass. The real runner validation prefix was tested,
as were duration/configuration preservation and controller handoff of both systems.
Evidence: `tests/results/2026-11-04-capacity110k-preparation/` in the repository root.

This prepares a higher offered load to bracket Kafka's sustainable ceiling. It
is not a measured capacity result. Nothing was deployed over the active 85k
sequence; its frozen artifacts remain the comparison contract. The broker's
previous 100k overload remains evidence against claiming sustained 100k, regardless
of whether a later 110k test is run. Any overloaded trial must be retained and
reported, not averaged into a successful capacity claim.
