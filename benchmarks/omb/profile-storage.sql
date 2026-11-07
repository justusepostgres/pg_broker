-- Run after a measurement, not during it. This reports physical representation,
-- not CPU cost or a predicted speedup. No records or configuration are changed.
BEGIN READ ONLY;
SET LOCAL statement_timeout = '3s';
SHOW default_toast_compression;

SELECT attname, attstorage, attcompression
FROM pg_attribute
WHERE attrelid = 'broker.batches'::regclass
  AND attname IN ('data', 'timestamps') ORDER BY attname;

WITH chosen AS (
    SELECT name FROM broker.topics ORDER BY name LIMIT 1
), sample AS (
    SELECT batch.* FROM chosen
    CROSS JOIN LATERAL (
        SELECT base_offset, record_count, data, timestamps
        FROM broker.batches
        WHERE topic = chosen.name AND partition = 0
        ORDER BY base_offset DESC LIMIT 100
    ) batch
)
SELECT pg_column_compression(data) AS compression,
       count(*) AS batches, sum(record_count) AS records,
       min(base_offset) AS first_offset, max(base_offset) AS last_offset,
       round(avg(octet_length(data))) AS average_record_bytes,
       round(avg(pg_column_size(data))) AS average_stored_bytes,
       round(avg(pg_column_size(timestamps))) AS average_stored_timestamp_bytes
FROM sample GROUP BY pg_column_compression(data);

SELECT pg_size_pretty(pg_total_relation_size('broker.batches')) AS total_batch_storage;
COMMIT;
