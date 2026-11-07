-- Experimental read-only replication handles. Existing journal operations stay
-- unchanged. A complete read must never silently return a bounded prefix.
CREATE FUNCTION public.broker_raft_log_read(k text,e bigint,q jsonb,complete boolean) RETURNS jsonb
LANGUAGE plpgsql VOLATILE STRICT SECURITY DEFINER SET search_path=pg_catalog,broker AS $$
DECLARE first_idx numeric; last_idx numeric; row_value record; accepted_idx numeric;
 result jsonb := '[]';
 used bigint := 2; item_size integer; count integer := 0;
 max_entries integer := CASE WHEN complete THEN 4096 ELSE 256 END;
 max_bytes integer := CASE WHEN complete THEN 67108864 ELSE 8388608 END;
BEGIN
 IF octet_length(q::text)>4096 THEN RAISE EXCEPTION 'Invalid Raft read bounds' USING ERRCODE='22023'; END IF;
 PERFORM 1 FROM broker.raft_votes WHERE group_key=k AND writer_epoch=e FOR SHARE;
 IF NOT FOUND THEN RAISE EXCEPTION 'Raft reader fenced' USING ERRCODE='55000'; END IF;
 IF q->'empty'='true'::jsonb THEN RETURN result; END IF;
 first_idx := broker.raft_log_index(q->'start');
 last_idx := broker.raft_log_index(q->'end');
 IF first_idx>last_idx THEN RAISE EXCEPTION 'Invalid Raft read bounds' USING ERRCODE='22023'; END IF;
 FOR row_value IN SELECT log_index,entry FROM broker.raft_log_entries
  WHERE group_key=k AND log_index>=first_idx AND log_index<=last_idx
  ORDER BY log_index LIMIT max_entries+1 LOOP
  item_size := octet_length(row_value.entry::text)+2;
  IF count=max_entries OR used+item_size>max_bytes THEN
   IF complete THEN RAISE EXCEPTION 'Complete Raft read exceeds bounded response' USING ERRCODE='54000'; END IF;
   EXIT;
  END IF;
  accepted_idx := row_value.log_index; used := used+item_size; count := count+1;
 END LOOP;
 -- Aggregate once, rather than repeatedly copying a growing JSON array. The
 -- shared owner lock keeps both scans consistent with all journal writers.
 IF accepted_idx IS NOT NULL THEN
  SELECT jsonb_agg(entry ORDER BY log_index) INTO result FROM broker.raft_log_entries
   WHERE group_key=k AND log_index>=first_idx AND log_index<=accepted_idx;
 END IF;
 RETURN result;
END $$;
REVOKE ALL ON FUNCTION public.broker_raft_log_read(text,bigint,jsonb,boolean) FROM PUBLIC;
