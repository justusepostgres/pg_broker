\set ON_ERROR_STOP on
-- Read-only diagnostic AFTER traffic stops. Compare the current no-op retention
-- boundary scan with a bounded first-live-row seek. Neither query deletes data.
BEGIN READ ONLY;
SET LOCAL statement_timeout = '3s';
SELECT name AS topic FROM broker.topics ORDER BY name LIMIT 1 \gset
-- Current aggregate scans the entire bounded page even if row one is live.
EXPLAIN (ANALYZE, BUFFERS, TIMING OFF)
SELECT min(base_offset) FILTER (WHERE appended_at >= clock_timestamp()-interval '7 days'),
       max(base_offset+record_count)
FROM (SELECT base_offset, record_count, appended_at FROM broker.batches
      WHERE topic=:'topic' AND partition=0 ORDER BY base_offset LIMIT 4096) page;
-- A candidate early exit for the first live row, still bounded to 4096 rows.
-- No-live-row handling would still need the last expired batch's endpoint.
EXPLAIN (ANALYZE, BUFFERS, TIMING OFF)
SELECT base_offset FROM (
 SELECT base_offset, appended_at FROM broker.batches
 WHERE topic=:'topic' AND partition=0 ORDER BY base_offset LIMIT 4096
) page
WHERE appended_at >= clock_timestamp()-interval '7 days'
ORDER BY base_offset LIMIT 1;
COMMIT;
