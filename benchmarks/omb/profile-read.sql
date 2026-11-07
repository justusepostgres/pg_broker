-- Post-run diagnostic only; excludes gateway parsing and network serialization.
\set ON_ERROR_STOP on
BEGIN READ ONLY;
SET LOCAL statement_timeout = '10s';
SET LOCAL track_functions = 'all';
EXPLAIN (ANALYZE, BUFFERS)
WITH requests AS MATERIALIZED (
    SELECT jsonb_build_object('max', 4194304, 'requests', jsonb_agg(
        jsonb_build_object('topic', p.topic, 'partition', p.partition,
                          'offset', tail.base_offset, 'max', 1048576)
        ORDER BY p.partition)) AS request
    FROM broker.partitions p
    CROSS JOIN LATERAL (
        SELECT base_offset FROM broker.batches b
        WHERE b.topic = p.topic AND b.partition = p.partition
        ORDER BY base_offset DESC LIMIT 1
    ) tail
    WHERE p.topic = (SELECT name FROM broker.topics ORDER BY name LIMIT 1)
)
SELECT public.broker_fetch_many(request)
FROM requests CROSS JOIN generate_series(1, 10);
SELECT schemaname, funcname, calls, total_time, self_time
FROM pg_stat_xact_user_functions ORDER BY total_time DESC;
COMMIT;
