-- Post-run only. Measure the actual no-op dispatcher, rolling back all writes.
-- These warm-cache SQL timings are not end-to-end latency or durability tests.
\set ON_ERROR_STOP on
SELECT name AS sampled_topic FROM broker.topics ORDER BY name LIMIT 1 \gset
BEGIN;
SET LOCAL statement_timeout='3s';
EXPLAIN (ANALYZE, BUFFERS, TIMING)
SELECT public.broker_call(jsonb_build_object('op','retain','topic',:'sampled_topic',
 'partition',0,'retention_ms',604800000));
ROLLBACK;
