-- Experimental, local Raft journal primitives. Not a replicated state machine.
CREATE TABLE broker.raft_log_state (
 group_key text PRIMARY KEY REFERENCES broker.raft_votes ON DELETE CASCADE,
 purged jsonb,
 committed jsonb
);
CREATE TABLE broker.raft_log_entries (
 group_key text REFERENCES broker.raft_log_state ON DELETE CASCADE,
 log_index numeric NOT NULL CHECK(log_index>=0 AND log_index<=18446744073709551615 AND log_index=trunc(log_index)),
 entry jsonb NOT NULL CHECK(jsonb_typeof(entry)='object' AND octet_length(entry::text)<=1048576),
 PRIMARY KEY(group_key,log_index)
);
SELECT pg_catalog.pg_extension_config_dump('broker.raft_log_state','');
SELECT pg_catalog.pg_extension_config_dump('broker.raft_log_entries','');

-- Unconstrained numeric avoids rounding fractional input before validation and
-- preserves the complete u64 index range. Typed log IDs/payloads belong to the
-- future Rust adapter; callers of this internal API must be trusted.
CREATE FUNCTION broker.raft_log_index(id jsonb) RETURNS numeric
LANGUAGE plpgsql IMMUTABLE SET search_path=pg_catalog AS $$
DECLARE n numeric;
BEGIN
 IF id IS NULL OR jsonb_typeof(id)<>'object' OR jsonb_typeof(id->'index') IS DISTINCT FROM 'number'
    OR NOT (id->>'index' ~ '^(0|[1-9][0-9]{0,19})$') OR octet_length(id::text)>1024 THEN
  RAISE EXCEPTION 'Invalid Raft log ID' USING ERRCODE='22023';
 END IF;
 n := (id->>'index')::numeric;
 IF n>18446744073709551615 THEN RAISE EXCEPTION 'Raft index overflow' USING ERRCODE='22023'; END IF;
 RETURN n;
END $$;
REVOKE ALL ON FUNCTION broker.raft_log_index(jsonb) FROM PUBLIC;

CREATE FUNCTION public.broker_raft_log(k text,e bigint,op text,q jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE STRICT SECURITY DEFINER SET search_path=pg_catalog,broker AS $$
DECLARE s broker.raft_log_state; item jsonb; existing jsonb; result jsonb := '[]';
 idx numeric; tail numeric; boundary numeric; first_idx numeric; last_idx numeric;
 used integer := 2; item_size integer; amount integer;
BEGIN
 IF octet_length(q::text)>8388608 THEN RAISE EXCEPTION 'Raft request too large' USING ERRCODE='22023'; END IF;
 -- The same row serializes log and vote operations, including a new owner claim.
 PERFORM 1 FROM broker.raft_votes WHERE group_key=k AND writer_epoch=e FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'Raft log writer fenced' USING ERRCODE='55000'; END IF;
 INSERT INTO broker.raft_log_state(group_key) VALUES(k) ON CONFLICT DO NOTHING;
 SELECT * INTO s FROM broker.raft_log_state WHERE group_key=k;
 boundary := CASE WHEN s.purged IS NULL THEN -1 ELSE broker.raft_log_index(s.purged) END;
 SELECT log_index INTO tail FROM broker.raft_log_entries WHERE group_key=k ORDER BY log_index DESC LIMIT 1;
 tail := coalesce(tail,boundary);
 IF op='append' THEN
  IF jsonb_typeof(q) IS DISTINCT FROM 'array' OR jsonb_array_length(q)>256 THEN
   RAISE EXCEPTION 'Invalid Raft append batch' USING ERRCODE='22023';
  END IF;
  FOR item IN SELECT value FROM jsonb_array_elements(q) LOOP
   idx := broker.raft_log_index(item->'log_id');
   IF NOT (item ? 'payload') OR octet_length(item::text)>1048576 THEN
    RAISE EXCEPTION 'Invalid Raft entry' USING ERRCODE='22023';
   END IF;
   IF idx<=boundary OR (last_idx IS NOT NULL AND idx<>last_idx+1) THEN
    RAISE EXCEPTION 'Raft append crosses purged prefix or is not consecutive' USING ERRCODE='22023';
   END IF;
   SELECT entry INTO existing FROM broker.raft_log_entries WHERE group_key=k AND log_index=idx;
   IF FOUND THEN
    IF existing<>item THEN RAISE EXCEPTION 'Truncate conflicting Raft suffix before append' USING ERRCODE='55000'; END IF;
   ELSE
    IF idx<>tail+1 THEN RAISE EXCEPTION 'Raft append would leave a hole' USING ERRCODE='22023'; END IF;
    INSERT INTO broker.raft_log_entries VALUES(k,idx,item);
    tail := idx;
   END IF;
   last_idx := idx;
  END LOOP;
 ELSIF op='truncate' THEN
  idx := broker.raft_log_index(q);
  IF idx<=boundary OR (s.committed IS NOT NULL AND idx<=broker.raft_log_index(s.committed)) THEN
   RAISE EXCEPTION 'Cannot truncate purged or committed Raft prefix' USING ERRCODE='55000';
  END IF;
  DELETE FROM broker.raft_log_entries WHERE group_key=k AND log_index>=idx;
 ELSIF op IN ('commit','purge') THEN
  idx := broker.raft_log_index(q);
  SELECT entry->'log_id' INTO existing FROM broker.raft_log_entries WHERE group_key=k AND log_index=idx;
  IF idx=boundary THEN existing := s.purged; END IF;
  IF existing IS DISTINCT FROM q THEN RAISE EXCEPTION 'Raft boundary does not match durable log' USING ERRCODE='55000'; END IF;
  IF op='commit' THEN
   IF s.committed IS NOT NULL AND idx<broker.raft_log_index(s.committed) THEN
    RAISE EXCEPTION 'Raft committed index must not regress' USING ERRCODE='55000';
   END IF;
   UPDATE broker.raft_log_state SET committed=q WHERE group_key=k;
  ELSE
   IF idx<boundary OR s.committed IS NULL OR idx>broker.raft_log_index(s.committed) THEN
    RAISE EXCEPTION 'Cannot purge beyond committed Raft prefix' USING ERRCODE='55000';
   END IF;
   -- Caller must also prove a durable applied state/snapshot before invoking.
   DELETE FROM broker.raft_log_entries WHERE group_key=k AND log_index<=idx;
   UPDATE broker.raft_log_state SET purged=q WHERE group_key=k;
  END IF;
 ELSIF op='read' THEN
  first_idx := broker.raft_log_index(q->'start');
  last_idx := broker.raft_log_index(q->'end');
  amount := (q->>'limit')::integer;
  IF amount IS NULL OR amount NOT BETWEEN 1 AND 256 OR last_idx<first_idx THEN
   RAISE EXCEPTION 'Invalid Raft read bounds' USING ERRCODE='22023';
  END IF;
  FOR item IN SELECT entry FROM broker.raft_log_entries
   WHERE group_key=k AND log_index>=first_idx AND log_index<=last_idx ORDER BY log_index LIMIT amount LOOP
   item_size := octet_length(item::text)+2; -- Include array separators in budget.
   IF used+item_size>8388608 THEN EXIT; END IF;
   result := result || jsonb_build_array(item); used := used+item_size;
  END LOOP;
  RETURN result;
 ELSIF op<>'state' THEN
  RAISE EXCEPTION 'Unknown Raft journal operation' USING ERRCODE='22023';
 END IF;
 SELECT * INTO s FROM broker.raft_log_state WHERE group_key=k;
 SELECT entry->'log_id' INTO existing FROM broker.raft_log_entries WHERE group_key=k ORDER BY log_index DESC LIMIT 1;
 RETURN jsonb_build_object('purged',s.purged,'committed',s.committed,'last',coalesce(existing,s.purged));
END $$;
REVOKE ALL ON FUNCTION public.broker_raft_log(text,bigint,text,jsonb) FROM PUBLIC;
