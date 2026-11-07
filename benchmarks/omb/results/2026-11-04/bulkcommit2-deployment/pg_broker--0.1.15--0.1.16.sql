-- Opt-in deterministic, bounded retention through committed journal entries.
-- The 0.1.15 public wrapper supplies private transaction authorization.
CREATE OR REPLACE FUNCTION broker.raft_partition_apply_inner(k text,e bigint,id jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE STRICT SECURITY DEFINER
SET search_path=pg_catalog,broker AS $$
DECLARE s broker.raft_partition_state; item jsonb; payload jsonb; command jsonb;
 idx numeric; previous numeric; committed jsonb; response jsonb; off bigint; actual bigint;
 append_ms bigint; finish bigint; earliest_now bigint; retained_from bigint; removed integer;
BEGIN
 PERFORM 1 FROM broker.raft_votes WHERE group_key=k AND writer_epoch=e FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'Raft application owner fenced' USING ERRCODE='55000'; END IF;
 SELECT * INTO s FROM broker.raft_partition_state WHERE group_key=k;
 IF NOT FOUND THEN RAISE EXCEPTION 'Replica partition is not bound' USING ERRCODE='55000'; END IF;
 idx := broker.raft_log_index(id);
 previous := CASE WHEN s.applied IS NULL THEN -1 ELSE broker.raft_log_index(s.applied) END;
 IF idx=previous AND id=s.applied THEN RETURN to_jsonb(s); END IF;
 IF idx<>previous+1 THEN RAISE EXCEPTION 'Application must advance consecutively' USING ERRCODE='55000'; END IF;
 SELECT l.committed INTO committed FROM broker.raft_log_state l WHERE group_key=k;
 IF committed IS NULL OR idx>broker.raft_log_index(committed) THEN
  RAISE EXCEPTION 'Cannot apply an uncommitted entry' USING ERRCODE='55000';
 END IF;
 SELECT entry INTO item FROM broker.raft_log_entries WHERE group_key=k AND log_index=idx;
 IF NOT FOUND OR item->'log_id' IS DISTINCT FROM id THEN
  RAISE EXCEPTION 'Application entry missing or changed' USING ERRCODE='55000';
 END IF;
 payload := item->'payload';
 IF payload='"Blank"'::jsonb THEN
  response := 'null'::jsonb;
 ELSIF jsonb_typeof(payload)='object' AND payload ? 'Membership' AND (SELECT count(*) FROM jsonb_object_keys(payload))=1 THEN
  s.membership := jsonb_build_object('log_id',id,'membership',payload->'Membership');
  response := 'null'::jsonb;
 ELSIF jsonb_typeof(payload)='object' AND payload ? 'Normal'
   AND (SELECT count(*) FROM jsonb_object_keys(payload))=1
   AND payload->'Normal'->>'op'='retain' THEN
  command := payload->'Normal';
  IF jsonb_typeof(command) IS DISTINCT FROM 'object'
     OR (SELECT count(*) FROM jsonb_object_keys(command))<>3
     OR jsonb_typeof(command->'expected_earliest') IS DISTINCT FROM 'number'
     OR jsonb_typeof(command->'new_earliest') IS DISTINCT FROM 'number'
     OR NOT(command->>'expected_earliest' ~ '^(0|[1-9][0-9]{0,18})$')
     OR NOT(command->>'new_earliest' ~ '^(0|[1-9][0-9]{0,18})$') THEN
   RAISE EXCEPTION 'Invalid replica retention command' USING ERRCODE='22023';
  END IF;
  off := (command->>'expected_earliest')::bigint;
  retained_from := (command->>'new_earliest')::bigint;
  PERFORM 1 FROM broker.topics WHERE name=s.topic AND topic_id=s.topic_id FOR KEY SHARE;
  IF NOT FOUND THEN RAISE EXCEPTION 'Replica topic identity mismatch' USING ERRCODE='55000'; END IF;
  SELECT earliest,next_offset INTO earliest_now,finish FROM broker.partitions
   WHERE topic=s.topic AND partition=s.partition FOR UPDATE;
  IF NOT FOUND OR earliest_now<>off OR retained_from<off OR retained_from>finish THEN
   RAISE EXCEPTION 'Replica retention boundary diverged' USING ERRCODE='55000';
  END IF;
  IF retained_from<>finish AND NOT EXISTS(SELECT 1 FROM broker.batches
     WHERE topic=s.topic AND partition=s.partition AND base_offset=retained_from) THEN
   RAISE EXCEPTION 'Retention requires a batch boundary' USING ERRCODE='22023';
  END IF;
  SELECT count(*) INTO removed FROM (SELECT 1 FROM broker.batches
    WHERE topic=s.topic AND partition=s.partition AND base_offset<retained_from LIMIT 4097) bounded;
  IF removed>4096 THEN RAISE EXCEPTION 'Replica retention batch limit' USING ERRCODE='54000'; END IF;
  DELETE FROM broker.batches WHERE topic=s.topic AND partition=s.partition AND base_offset<retained_from;
  UPDATE broker.partitions SET earliest=retained_from WHERE topic=s.topic AND partition=s.partition;
  response := jsonb_build_object('error',0,'offset',retained_from);
 ELSIF jsonb_typeof(payload)='object' AND payload ? 'Normal' AND (SELECT count(*) FROM jsonb_object_keys(payload))=1 THEN
  command := payload->'Normal';
  IF jsonb_typeof(command) IS DISTINCT FROM 'object' OR command->>'op' IS DISTINCT FROM 'append'
     OR jsonb_typeof(command->'data') IS DISTINCT FROM 'string'
     OR jsonb_typeof(command->'expected_offset') IS DISTINCT FROM 'number'
     OR jsonb_typeof(command->'appended_at_ms') IS DISTINCT FROM 'number'
     OR NOT(command->>'expected_offset' ~ '^(0|[1-9][0-9]{0,18})$')
     OR NOT(command->>'appended_at_ms' ~ '^(0|[1-9][0-9]{0,14})$') THEN
   RAISE EXCEPTION 'Invalid replica append command' USING ERRCODE='22023';
  END IF;
  off := (command->>'expected_offset')::bigint;
  append_ms := (command->>'appended_at_ms')::bigint;
  IF append_ms>253402300799999 THEN RAISE EXCEPTION 'Append time outside supported range' USING ERRCODE='22023'; END IF;
  PERFORM 1 FROM broker.topics WHERE name=s.topic AND topic_id=s.topic_id FOR KEY SHARE;
  IF NOT FOUND THEN RAISE EXCEPTION 'Replica topic identity mismatch' USING ERRCODE='55000'; END IF;
  SELECT next_offset INTO actual FROM broker.partitions WHERE topic=s.topic AND partition=s.partition FOR UPDATE;
  IF NOT FOUND OR actual<>off THEN RAISE EXCEPTION 'Replica offset diverged' USING ERRCODE='55000'; END IF;
  response := public.broker_append_identified(s.topic,s.partition,s.topic_id,decode(command->>'data','hex'));
  IF response->>'error' IS DISTINCT FROM '0' THEN
   RAISE EXCEPTION 'Committed replica record set is invalid: %',response USING ERRCODE='22023';
  END IF;
  SELECT next_offset INTO finish FROM broker.partitions WHERE topic=s.topic AND partition=s.partition;
  -- Retention time comes from the replicated command, never each follower's clock.
  UPDATE broker.batches SET appended_at = 'epoch'::timestamptz + append_ms * interval '1 millisecond'
   WHERE topic=s.topic AND partition=s.partition AND base_offset>=off AND base_offset<finish;
 ELSE
  RAISE EXCEPTION 'Unsupported replica payload' USING ERRCODE='22023';
 END IF;
 UPDATE broker.raft_partition_state SET applied=id,membership=s.membership,result=response
  WHERE group_key=k RETURNING * INTO s;
 RETURN to_jsonb(s);
END $$;
