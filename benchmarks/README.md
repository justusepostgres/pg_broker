# Kafka round-trip measurements

Use Python 3.11 and the pinned `tests/requirements.lock` environment:

```sh
.venv/bin/python benchmarks/kafka_roundtrip.py \
  --bootstrap localhost:9092 --records 100000 --bytes 256 --partitions 4 \
  --label 'describe hosts, PostgreSQL settings and replication here'
```

The ordinary confluent-kafka 2.6.1 producer uses `acks=all`, 5 ms linger,
256 KiB batches and a bounded queue of 1,024 records / 64 MiB. When full,
it drains to 512 queued messages before refilling, preventing a one-slot refill
loop from collapsing the intended workload into single-record batches. A separate
consumer then reads all records using manual partition assignment, checks
identities and contiguous offsets, and commits. Every run creates a unique topic
and deletes it after success. Failed runs retain their topic for investigation.

The JSON result separates producing and consuming rates. Acknowledgment latency
runs from a successful enqueue attempt to the delivery callback and includes
client batching/queue time. It excludes time waiting for room before enqueue.
Callback polling and the Python runtime are included. These are batch-workload
measurements of the current **debug-profile** container builds, not a steady-state or end-to-end service-level guarantee.

Record the database durability configuration, host count and specifications,
record size, partition count, client placement and repetitions alongside results.
A loopback development run does not measure synchronous replication or network
failure recovery. Do not compare results across different configurations without
accounting for those differences.

For the pinned upstream Java benchmark, see [OMB](omb/README.md). The historical
measurements above used debug builds; current Docker builds use release. Their
results must be labeled separately.
