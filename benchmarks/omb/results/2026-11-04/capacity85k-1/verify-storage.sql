\set ON_ERROR_STOP on
SELECT extversion, current_setting('fsync'),current_setting('full_page_writes'),current_setting('synchronous_commit'),has_function_privilege('public.broker_fetch_binary(jsonb)','EXECUTE'),has_function_privilege('public.broker_append_bytes(text,integer,bytea)','EXECUTE') FROM pg_extension WHERE extname='pg_broker';
WITH batches AS (
 SELECT topic,partition,base_offset,record_count,
 lag(base_offset+record_count,1,0::bigint) OVER(PARTITION BY topic,partition ORDER BY base_offset) AS prior_end
 FROM broker.batches
), totals AS (
 SELECT topic,partition,max(base_offset+record_count) AS last_offset,
 bool_and(base_offset=prior_end) AS contiguous FROM batches GROUP BY topic,partition
)
SELECT count(*),bool_and(t.contiguous AND t.last_offset=p.next_offset AND p.earliest=0)
 FROM broker.partitions p JOIN totals t USING(topic,partition);
