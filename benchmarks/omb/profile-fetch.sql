-- Diagnostic only: a temporary table, rolled back at the end. Run with psql -v
-- ON_ERROR_STOP=1 against a disposable PostgreSQL database. This isolates index
-- access, not broker throughput; payload decoding and replication are absent.
BEGIN;
CREATE TEMP TABLE fetch_profile (
 topic text, partition int, base_offset bigint, record_count int,
 PRIMARY KEY(topic,partition,base_offset)
);
INSERT INTO fetch_profile
 SELECT 'profile',0,i*10,10 FROM generate_series(0,99999) i;
ANALYZE fetch_profile;

-- Current predicate must filter older batches within the partition.
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM fetch_profile WHERE topic='profile' AND partition=0
 AND base_offset+record_count>999995 ORDER BY base_offset LIMIT 4096;

-- Candidate: seek to the batch covering the offset, then scan forward.
-- Including the preceding batch preserves reads starting inside a batch.
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM fetch_profile WHERE topic='profile' AND partition=0
 AND base_offset >= coalesce((
   SELECT base_offset FROM fetch_profile WHERE topic='profile' AND partition=0
    AND base_offset<=999995 ORDER BY base_offset DESC LIMIT 1
 ),999995)
 AND base_offset+record_count>999995 ORDER BY base_offset LIMIT 4096;
ROLLBACK;
