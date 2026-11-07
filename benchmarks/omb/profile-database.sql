\set ON_ERROR_STOP on
-- Capture AFTER benchmark traffic stops. Cumulative counters cover the whole
-- fresh database lifecycle, including warmup, initialization and restarts.
-- Timing counters can be zero because their tracking setting is disabled.
BEGIN READ ONLY;
SET LOCAL statement_timeout = '3s';
SELECT jsonb_pretty(jsonb_build_object(
 'captured_at', clock_timestamp(),
 'scope', 'cumulative database lifecycle; not measurement-window deltas',
 'settings', jsonb_build_object(
   'server_version', current_setting('server_version'),
   'track_io_timing', current_setting('track_io_timing'),
   'track_wal_io_timing', current_setting('track_wal_io_timing'),
   'checkpoint_timeout', current_setting('checkpoint_timeout'),
   'max_wal_size', current_setting('max_wal_size'),
   'wal_compression', current_setting('wal_compression'),
   'default_toast_compression', current_setting('default_toast_compression')),
 'database', (SELECT to_jsonb(d) FROM pg_stat_database d WHERE datname=current_database()),
 'checkpointer', (SELECT to_jsonb(c) FROM pg_stat_checkpointer c),
 'wal', (SELECT to_jsonb(w) FROM pg_stat_wal w),
 'tables', (SELECT jsonb_agg(to_jsonb(t) ORDER BY relname) FROM pg_stat_user_tables t WHERE schemaname='broker'),
 'io', (SELECT jsonb_agg(to_jsonb(i) ORDER BY backend_type, object, context) FROM pg_stat_io i)
));
COMMIT;
