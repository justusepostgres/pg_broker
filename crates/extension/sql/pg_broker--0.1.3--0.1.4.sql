-- Return record bytes as bytea rows instead of constructing a hex/JSON document.
-- Every request emits one status row, followed by zero or more record batches.
CREATE FUNCTION public.broker_fetch_binary(q jsonb)
RETURNS TABLE(request_index integer, error integer, high bigint, earliest bigint,
              base_offset bigint, data bytea)
LANGUAGE plpgsql VOLATILE STRICT PARALLEL UNSAFE SECURITY DEFINER
SET search_path=pg_catalog,broker SET work_mem='16MB' AS $$
DECLARE req record; part broker.partitions%ROWTYPE; batch record;
 used integer=0; partition_used integer; budget integer; lim integer; off bigint;
BEGIN
 IF jsonb_typeof(q->'requests') IS DISTINCT FROM 'array' THEN
  RAISE EXCEPTION 'requests must be an array' USING ERRCODE='22023';
 END IF;
 IF jsonb_array_length(q->'requests')>4096 OR EXISTS(
  SELECT 1 FROM jsonb_array_elements(q->'requests') item WHERE jsonb_typeof(item)<>'object'
 ) THEN RAISE EXCEPTION 'invalid fetch requests' USING ERRCODE='22023'; END IF;
 budget=least(greatest(coalesce((q->>'max')::integer,4194304),1),4194304);
 FOR req IN SELECT value, ordinality FROM jsonb_array_elements(q->'requests') WITH ORDINALITY LOOP
  request_index=(req.ordinality-1)::integer;
  error=0; high=0; earliest=0; base_offset=NULL; data=NULL;
  SELECT p.* INTO part FROM broker.partitions p
   WHERE p.topic=req.value->>'topic' AND p.partition=(req.value->>'partition')::integer;
  IF NOT FOUND THEN error=3; RETURN NEXT; CONTINUE; END IF;
  high=part.next_offset; earliest=part.earliest;
  off=(req.value->>'offset')::bigint;
  IF off IS NULL THEN RAISE EXCEPTION 'offset is required' USING ERRCODE='22023'; END IF;
  IF off<part.earliest OR off>part.next_offset THEN error=1; RETURN NEXT; CONTINUE; END IF;
  RETURN NEXT;
  partition_used=0;
  lim=least(greatest(coalesce((req.value->>'max')::integer,1048576),0),1048576,greatest(budget-used,0));
  IF lim=0 THEN CONTINUE; END IF;
  FOR batch IN SELECT b.base_offset,b.data FROM broker.batches b
   WHERE b.topic=part.topic AND b.partition=part.partition
    AND b.base_offset>=coalesce((SELECT b2.base_offset FROM broker.batches b2
     WHERE b2.topic=part.topic AND b2.partition=part.partition AND b2.base_offset<=off
     ORDER BY b2.base_offset DESC LIMIT 1),off)
    AND b.base_offset+b.record_count>off
   ORDER BY b.base_offset LIMIT 4096 LOOP
   EXIT WHEN used>0 AND partition_used+octet_length(batch.data)>lim;
   base_offset=batch.base_offset; data=batch.data; RETURN NEXT;
   partition_used=partition_used+octet_length(batch.data);
   used=used+octet_length(batch.data);
  END LOOP;
 END LOOP;
END $$;
REVOKE ALL ON FUNCTION public.broker_fetch_binary(jsonb) FROM PUBLIC;
