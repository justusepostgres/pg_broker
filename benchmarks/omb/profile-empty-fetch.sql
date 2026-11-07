-- Post-run, read-only diagnostic. No network/client throughput claim.
\set ON_ERROR_STOP on
BEGIN READ ONLY;
SET LOCAL statement_timeout='10s';
SET LOCAL track_functions='all';
EXPLAIN (ANALYZE, BUFFERS)
WITH request AS MATERIALIZED (
 SELECT jsonb_build_object('max',4194304,'requests',jsonb_agg(
  jsonb_build_object('topic',topic,'partition',partition,'offset',next_offset,'max',1048576)
  ORDER BY partition)) AS q
 FROM broker.partitions WHERE topic=(SELECT name FROM broker.topics ORDER BY name LIMIT 1)
)
SELECT count(*) FROM request CROSS JOIN generate_series(1,100) iteration
CROSS JOIN LATERAL public.broker_fetch_binary(q || jsonb_build_object('_iteration',iteration)) result;
SELECT schemaname,funcname,calls,total_time,self_time FROM pg_stat_xact_user_functions ORDER BY total_time DESC;
COMMIT;
