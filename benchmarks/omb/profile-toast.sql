\set ON_ERROR_STOP on
-- Diagnostic only, AFTER benchmark traffic stops. No broker rows/settings change.
-- Temporary writes omit durable WAL/fsync; this is not a throughput comparison.
BEGIN;
SET LOCAL statement_timeout = '10s';
SET LOCAL lock_timeout = '2s';
SET LOCAL work_mem = '16MB';
SET LOCAL temp_file_limit = '128MB';
CREATE TEMP TABLE omb_toast_sample(id integer, data bytea) ON COMMIT DROP;
ALTER TABLE omb_toast_sample ALTER data SET STORAGE EXTERNAL;
INSERT INTO omb_toast_sample
SELECT row_number() OVER ()::integer, data || ''::bytea FROM (
 SELECT data FROM broker.batches
 WHERE topic=(SELECT name FROM broker.topics ORDER BY name LIMIT 1)
 AND partition=0 ORDER BY base_offset DESC LIMIT 100
) sample;
DO $$ BEGIN
 IF NOT EXISTS (SELECT FROM omb_toast_sample) THEN
  RAISE EXCEPTION 'profile requires existing benchmark records';
 END IF;
END $$;
CREATE TEMP TABLE omb_toast_pglz(id integer, data bytea COMPRESSION pglz) ON COMMIT DROP;
CREATE TEMP TABLE omb_toast_external(id integer, data bytea) ON COMMIT DROP;
ALTER TABLE omb_toast_external ALTER data SET STORAGE EXTERNAL;
-- Concatenation forces a fresh datum, avoiding reuse of already-compressed data.
EXPLAIN (ANALYZE, BUFFERS, TIMING OFF)
INSERT INTO omb_toast_pglz SELECT id, data || ''::bytea FROM omb_toast_sample;
EXPLAIN (ANALYZE, BUFFERS, TIMING OFF)
INSERT INTO omb_toast_external SELECT id, data || ''::bytea FROM omb_toast_sample;
SELECT 'pglz' AS variant, count(*) AS batches,
 sum(octet_length(data)) AS logical_bytes, sum(pg_column_size(data)) AS stored_value_bytes,
 pg_total_relation_size('omb_toast_pglz') AS relation_bytes FROM omb_toast_pglz
UNION ALL
SELECT 'external', count(*), sum(octet_length(data)), sum(pg_column_size(data)),
 pg_total_relation_size('omb_toast_external') FROM omb_toast_external;
-- Read and reconstruct every value; octet_length(data) alone need not detoast it.
EXPLAIN (ANALYZE, BUFFERS, TIMING OFF)
SELECT sum(octet_length(data || ''::bytea)) FROM omb_toast_pglz;
EXPLAIN (ANALYZE, BUFFERS, TIMING OFF)
SELECT sum(octet_length(data || ''::bytea)) FROM omb_toast_external;
SELECT count(*) AS checked_batches,
 bool_and(p.data = s.data AND e.data = s.data) AS identical_bytes
FROM omb_toast_sample s JOIN omb_toast_pglz p USING(id)
JOIN omb_toast_external e USING(id);
ROLLBACK;
