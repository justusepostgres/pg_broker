# Kafka compatibility contract

Targets: **kafka-python 2.0.2** and **confluent-kafka 2.6.1 (librdkafka 2.6.1)**, Python 3.11. No patches, custom serializers or forced `api_version` are required. ApiVersions advertises exactly the following ranges. Metadata v4 permits the target client to infer Kafka 0.11; it does not imply support for every Kafka 0.11 feature.

| Key | API | Versions | Implemented behavior / limits |
|---:|---|---|---|
| 18 | ApiVersions | 0–3 | Exact capability list; flexible v3 negotiation with bounded tagged fields |
| 3 | Metadata | 0–4 | One logical broker/controller, explicit topics or listing, fixed partitions, unknown-topic errors; auto-create flag never creates topics |
| 19 | CreateTopics | 0 | Explicit 1–128 partitions, replication factor 1; custom assignments/configs rejected |
| 20 | DeleteTopics | 0 | Transactional cascading removal of logs and committed offsets; subscribed groups rebalanced |
| 0 | Produce | 3 | Multiple uncompressed magic-2 batches per selected partition, validated framing/CRC, keys, nullable values, timestamps, headers; acks 0/1/all |
| 1 | Fetch | 4 | Offset fetch, high watermark, bounded long poll, read-uncommitted and read-committed (all accepted batches are nontransactional); no replica fetch |
| 2 | ListOffsets | 1 | Earliest (-2), latest (-1), first record with timestamp >= requested timestamp |
| 10 | FindCoordinator | 0 | Shared classic-group coordinator at advertised endpoint |
| 11 | JoinGroup | 0–2 | Durable generations, dynamic members, consumer protocol with `range` offered; bounded session/rebalance timeouts |
| 14 | SyncGroup | 0–1 | Client leader's eager range assignment, persisted atomically, checked for exclusive ownership |
| 12 | Heartbeat | 0–1 | Durable membership, expiry, generation checks and rebalance notification |
| 13 | LeaveGroup | 0–1 | Removes member and starts a fenced generation |
| 8 | OffsetCommit | 2 | Next offset plus metadata; managed member/generation/ownership validation; unmanaged commit only without active managed members |
| 9 | OffsetFetch | 1 | Group-specific committed offset and metadata, -1 if absent |

Unsupported API keys/versions close the connection. The server cannot encode a generic response for an unknown schema. Unsupported compression, transaction/control attributes, invalid batch counts or CRCs yield `CORRUPT_MESSAGE (2)`; do not enable these producer features. `transactional_id` and replica Fetch are rejected by closing the connection. These restrictions are intentional and are not hidden by advertising more APIs.

Expected domain error mappings:

| Code | Meaning here |
|---:|---|
| 1 | Offset outside retained bounds, or negative committed offset |
| 2 | Invalid or unsupported record-batch representation |
| 3 | Unknown topic/partition |
| 17 | Invalid topic name |
| 22 | Stale generation, or unmanaged commit to an active managed group |
| 23 | Incompatible group protocol, invalid assignment set, or group capacity exceeded |
| 25 | Unknown/expired member |
| 26 | Session timeout outside 3–300 seconds |
| 27 | Rebalance in progress, stale sync/ownership, or assignment not ready |
| 36 | Topic already exists |
| 37 | Invalid partition count |
| 38 | Unsupported replication factor |
| 40 | Unsupported topic configs/custom assignments |
| 42 | Invalid operation or topic capacity exceeded |

Unexpected PostgreSQL failures, transaction/lock timeouts and resource exhaustion disconnect the client rather than fabricating success. Partial multi-partition requests may already have committed earlier partitions; retries can duplicate records. There is no guarantee of a successful response for malformed input.

Raw version/fault-injection tests use the pinned client's own encoders. Ordinary producer, consumer and admin integration tests use the public APIs. Additional clients and newer versions require their own negotiated-API and behavioral tests before being claimed compatible.

Classic consumer subscription/assignment metadata versions 0–3 are accepted with the eager range assignor. This does not implement the newer ConsumerGroupHeartbeat protocol, cooperative assignment, static membership, or Kafka transactions.
