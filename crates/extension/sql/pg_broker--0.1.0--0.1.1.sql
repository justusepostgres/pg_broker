-- Upgrade is transactional and preserves all records and group generations.
CREATE INDEX batches_retention_time ON broker.batches(appended_at);
COMMENT ON SCHEMA broker IS 'pg_broker durable storage; mutate through broker_call';
