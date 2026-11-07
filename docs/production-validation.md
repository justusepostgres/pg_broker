# Recovery and load validation — 2026-11-02

This extends the [initial verification](verification.md). It does not declare the
MVP production-ready or fully Kafka-conformant.

## Environment

Three isolated OCI Ubuntu 24.04 ARM VMs, each **2 OCPUs, 12 GiB RAM, 80 GiB boot
disk**, placed in three fault domains in one availability domain. All three were
K3s **v1.35.1+k3s1** servers using embedded etcd. CNPG **1.30.1** had two controller
replicas on different hosts. Three PostgreSQL **17.5** instances used required
`ANY 1` synchronous replication. The two gateways and three database instances
had required hostname anti-affinity. Extension version was **0.1.1**.

The client Pod shared a host with a database standby and gateway. Gateway and
extension containers use the current **Rust debug profile**. PostgreSQL had
128 MiB shared buffers; database and gateway Pods each had a 512 MiB memory
limit and no CPU limit. Database connections used verified TLS; Kafka traffic
stayed inside the private cluster. No existing OKE resources were changed.

## Verified recovery

- A real `pg_dump` archive restored into a separate three-instance CNPG cluster,
  with fresh volumes, generated credentials and a separate gateway. Both
  kafka-python 2.0.2 and confluent-kafka 2.6.1 resumed their original groups at
  committed offset 4, replaying exactly offsets 4 and 5 on both partitions.
- Native restore regression additionally compared all six extension tables,
  checked nonzero earliest offsets and confirmed append resumed at the next
  offset after restore.
- OCI **STOP** abruptly powered off the current primary's VM. CNPG automatically
  promoted a surviving instance. Both clients recovered acknowledged records
  and committed offsets, and producing/consuming new records succeeded while
  the host was still down. The observed interval from the power-off request to
  successful consumer resume was **105.82 seconds**.
- The powered-off host was restarted and its former primary rejoined as a
  standby. All three database instances and both gateways were healthy afterward.
- The final PostgreSQL check showed both standbys streaming in the synchronous
  quorum; all five observed application connections used TLS.

The first drill recovered records and offsets but exposed two test-infrastructure
issues: `kubectl cp` preserved a checkpoint-file owner that prevented overwriting
it, and a temporary guest-firewall rule disappeared on reboot. Correcting the
checkpoint owner and persisting the private-network rule allowed the complete
repeat drill to pass. The test now checks checkpoint writability before stopping
a host. These failures were not hidden by treating the first partial run as a pass.

## Gateway network-failure fix

A PostgreSQL statement timeout cannot protect a gateway waiting on a silent TCP
connection. Database work now has a **ten-second gateway deadline**, including a
five-second COMMIT confirmation limit. An ownership guard discards any physical
connection whose transaction did not finish successfully, including cancellation
by the four-second readiness timeout. Only confirmed transactions return their
connections to the pool.

A TCP proxy regression drops traffic at `START TRANSACTION`, verifies the Kafka
connection closes at the deadline, then restores traffic and proves recovery.
It separately cancels database work through `/ready` and verifies that subsequent
requests use a healthy connection. This retains the previous rule that a lost
COMMIT confirmation is ambiguous and must never produce a successful Kafka ack.

The updated native suite passed **28 tests in 64.61 seconds**, including database
crash recovery, missing synchronous standby, restore, groups, fencing, retention
and network blackholes. Workspace Clippy with warnings denied and formatting
checks passed. The same gateway source was rebuilt and rolled out to the cloud
cluster before the final host-loss and throughput checks.

## Bounded workload measurements

Ordinary confluent-kafka/librdkafka **2.6.1**, `acks=all`, four partitions,
5 ms linger, 256 KiB target batches, 1,024-message / 64 MiB queue limit. At queue
capacity, the harness drains to 512 queued messages before refilling. Producing
and consuming were separate stages; consumption used manual assignment and
validated unique record identities and contiguous partition offsets before
committing. Every completed run validated every expected record.

| Workload | Records | Produce records/s | Consume records/s | Ack p99, ms |
|---|---:|---:|---:|---:|
| 256-byte values, trial 1 | 100,000 | 4,216.2 | 9,746.0 | 248.074 |
| 256-byte values, trial 2 | 100,000 | 4,071.7 | 10,816.1 | 256.044 |
| 256-byte values, trial 3 | 100,000 | 4,077.0 | 10,835.4 | 258.903 |
| 4 KiB values | 10,000 | 346.8 | 741.7 | 2,951.683 |

Median produce throughput for the three 256-byte trials was **4,077 records/s**.
Latency includes producer queueing, batching and callback polling after successful
enqueue; it excludes waiting for admission to the bounded queue. This is not
server-only latency or a production SLA. Debug builds, payload size, co-located
clients, synchronous storage and Python overhead all affect these results.

An initial diagnostic run refilled one freed queue slot at a time and degenerated
into mostly single-record batches. It was deliberately stopped, not counted as a
completed throughput result. The corrected harness first passed a 10,000-record
cloud calibration before the four measured runs above.

## Reproduce and remaining work

- [HA overlay and hard host-loss drill](../deploy/cnpg-ha/README.md)
- [Logical backup and restore](backup-restore.md)
- [Benchmark commands and interpretation](../benchmarks/README.md)

Local raw evidence is under `.dev/ha-evidence/`, including build logs, the logical
archive, final topology/TLS checks, fault-test logs and benchmark JSON. Temporary
cloud resources are removed after verification; the source and test scripts remain.

This tested hard VM loss in one availability domain, not zone loss, asymmetric
network partitions, quorum loss or long soak behavior. Logical snapshots do not
provide continuous WAL recovery/PITR or off-site scheduled backup protection.
Release-profile throughput, sustained mixed workloads, additional client versions,
Kafka-side authentication/TLS and production operational hardening remain future
work. Keep at-least-once processing and duplicate-tolerant application semantics.
