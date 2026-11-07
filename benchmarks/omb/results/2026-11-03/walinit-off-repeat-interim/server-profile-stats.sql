\set ON_ERROR_STOP on
SELECT jsonb_build_object(
 'captured_at', clock_timestamp(),
 'database', (SELECT to_jsonb(s) FROM pg_stat_database s WHERE datname=current_database()),
 'wal', (SELECT to_jsonb(s) FROM pg_stat_wal s),
 'io', (SELECT jsonb_agg(to_jsonb(s)) FROM pg_stat_io s),
 'tables', (SELECT jsonb_agg(to_jsonb(s)) FROM pg_stat_user_tables s WHERE schemaname='broker'));
