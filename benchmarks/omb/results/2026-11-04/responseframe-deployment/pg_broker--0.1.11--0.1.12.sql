-- Opt-in local committed-entry application. No transport or snapshot support.
CREATE TABLE broker.raft_partition_state (
 group_key text PRIMARY KEY REFERENCES broker.raft_log_state ON DELETE CASCADE,
 topic text NOT NULL,
 topic_id uuid NOT NULL,
 partition integer NOT NULL,
 applied jsonb,
 membership jsonb NOT NULL DEFAULT '{"log_id":null,"membership":{"configs":[],"nodes":{}}}',
 result jsonb,
 UNIQUE(topic_id,partition)
);
SELECT pg_catalog.pg_extension_config_dump('broker.raft_partition_state','');

-- Bind only an empty partition. Topic provisioning is a separate control-plane
-- responsibility; this function neither invents an identity nor resets offsets.
CREATE FUNCTION public.broker_raft_partition_bind(k text,e bigint,t text,tid uuid,p integer)
RETURNS jsonb LANGUAGE plpgsql VOLATILE STRICT SECURITY DEFINER
SET search_path=pg_catalog,broker AS $$
DECLARE s broker.raft_partition_state; n bigint;
BEGIN
 PERFORM 1 FROM broker.raft_votes WHERE group_key=k AND writer_epoch=e FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'Raft application owner fenced' USING ERRCODE='55000'; END IF;
 SELECT * INTO s FROM broker.raft_partition_state WHERE group_key=k;
 IF FOUND THEN
  IF s.topic<>t OR s.topic_id<>tid OR s.partition<>p THEN
   RAISE EXCEPTION 'Replica binding is immutable' USING ERRCODE='55000';
  END IF;
  RETURN to_jsonb(s);
 END IF;
 PERFORM 1 FROM broker.topics WHERE name=t AND topic_id=tid FOR KEY SHARE;
 IF NOT FOUND THEN RAISE EXCEPTION 'Replica topic identity mismatch' USING ERRCODE='55000'; END IF;
 SELECT next_offset INTO n FROM broker.partitions WHERE topic=t AND partition=p FOR UPDATE;
 IF NOT FOUND OR n<>0 THEN RAISE EXCEPTION 'Replica requires an empty existing partition' USING ERRCODE='55000'; END IF;
 INSERT INTO broker.raft_partition_state(group_key,topic,topic_id,partition) VALUES(k,t,tid,p)
 RETURNING * INTO s;
 RETURN to_jsonb(s);
END $$;
REVOKE ALL ON FUNCTION public.broker_raft_partition_bind(text,bigint,text,uuid,integer) FROM PUBLIC;

CREATE FUNCTION public.broker_raft_partition_apply(k text,e bigint,id jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE STRICT SECURITY DEFINER
SET search_path=pg_catalog,broker AS $$
DECLARE s broker.raft_partition_state; item jsonb; payload jsonb; command jsonb;
 idx numeric; previous numeric; committed jsonb; response jsonb; off bigint; actual bigint;
 append_ms bigint; finish bigint;
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
REVOKE ALL ON FUNCTION public.broker_raft_partition_apply(text,bigint,jsonb) FROM PUBLIC;

-- A bound application must retain unapplied committed entries. This trigger
-- rolls back the entire journal purge, including its preceding row deletion.
CREATE FUNCTION broker.raft_partition_guard_purge() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog,broker AS $$
DECLARE applied_id jsonb;
BEGIN
 SELECT applied INTO applied_id FROM broker.raft_partition_state WHERE group_key=NEW.group_key;
 IF FOUND AND NEW.purged IS NOT NULL AND
    (applied_id IS NULL OR broker.raft_log_index(NEW.purged)>broker.raft_log_index(applied_id)) THEN
  RAISE EXCEPTION 'Cannot purge unapplied replica entries' USING ERRCODE='55000';
 END IF;
 RETURN NEW;
END $$;
REVOKE ALL ON FUNCTION broker.raft_partition_guard_purge() FROM PUBLIC;
CREATE TRIGGER raft_partition_guard_purge BEFORE UPDATE OF purged ON broker.raft_log_state
FOR EACH ROW EXECUTE FUNCTION broker.raft_partition_guard_purge();
