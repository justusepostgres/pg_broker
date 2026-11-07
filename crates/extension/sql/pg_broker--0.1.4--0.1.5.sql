-- This helper is reachable only through the validating pgrx entry point.
CREATE FUNCTION broker.append_bytes(t text,p integer,bytes bytea,n integer,maximum bigint,ts jsonb)
RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog,broker AS $$
DECLARE off bigint;
BEGIN
 SELECT next_offset INTO off FROM broker.partitions WHERE topic=t AND partition=p FOR UPDATE;
 IF NOT FOUND THEN RETURN '{"error":3}'; END IF;
 INSERT INTO broker.batches(topic,partition,base_offset,record_count,data,max_timestamp,timestamps)
 VALUES(t,p,off,n,bytes,maximum,ts);
 UPDATE broker.partitions SET next_offset=off+n WHERE topic=t AND partition=p;
 PERFORM pg_notify('pg_broker_records',t||':'||p::text);
 RETURN jsonb_build_object('error',0,'offset',off);
END $$;
REVOKE ALL ON FUNCTION broker.append_bytes(text,integer,bytea,integer,bigint,jsonb) FROM PUBLIC;
CREATE FUNCTION public.broker_append_bytes(topic text,partition integer,data bytea) RETURNS jsonb
AS 'MODULE_PATHNAME','broker_append_bytes_wrapper'
LANGUAGE c VOLATILE STRICT PARALLEL UNSAFE SECURITY DEFINER
SET search_path=pg_catalog,broker;
REVOKE ALL ON FUNCTION public.broker_append_bytes(text,integer,bytea) FROM PUBLIC;
