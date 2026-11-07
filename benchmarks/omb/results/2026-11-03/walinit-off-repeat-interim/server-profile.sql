-- Disposable diagnostic database only; each ALTER SYSTEM runs in autocommit.
\set ON_ERROR_STOP on
ALTER SYSTEM SET log_min_duration_statement = '10ms';
ALTER SYSTEM SET log_parameter_max_length = 0;
ALTER SYSTEM SET log_parameter_max_length_on_error = 0;
ALTER SYSTEM SET log_line_prefix = '%m [%p] %a ';
ALTER SYSTEM SET log_timezone = 'UTC';
ALTER SYSTEM SET log_checkpoints = on;
ALTER SYSTEM SET log_autovacuum_min_duration = 0;
ALTER SYSTEM SET track_io_timing = on;
ALTER SYSTEM SET track_wal_io_timing = on;
SELECT pg_reload_conf();
