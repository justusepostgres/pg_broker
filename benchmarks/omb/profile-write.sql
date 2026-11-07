-- Run with psql on a disposable benchmark database AFTER all traffic stops.
-- Copies one existing batch 100 times into a new topic, then rolls back all
-- writes. Measures execution costs only: excludes durable commit and network.
\set ON_ERROR_STOP on
SELECT 'profile_' || replace(gen_random_uuid()::text, '-', '') AS profile_topic \gset
BEGIN;
SET LOCAL statement_timeout = '10s';
SET LOCAL track_functions = 'all';
SELECT public.broker_call(jsonb_build_object(
    'op', 'create', 'topic', :'profile_topic', 'partitions', 1));

EXPLAIN (ANALYZE, BUFFERS, WAL)
WITH payload AS MATERIALIZED (
    SELECT encode(batch.data, 'hex') AS data
    FROM (SELECT name FROM broker.topics WHERE name <> :'profile_topic'
          ORDER BY name LIMIT 1) chosen
    CROSS JOIN LATERAL (
        SELECT data FROM broker.batches
        WHERE topic = chosen.name AND partition = 0
        ORDER BY base_offset DESC LIMIT 1
    ) batch
)
SELECT public.broker_call(jsonb_build_object(
    'op', 'append', 'topic', :'profile_topic', 'partition', 0, 'data', payload.data))
FROM payload CROSS JOIN generate_series(1, 100);

SELECT schemaname, funcname, calls, total_time, self_time
FROM pg_stat_xact_user_functions ORDER BY total_time DESC;
ROLLBACK;
