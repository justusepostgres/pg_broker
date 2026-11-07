-- Experimental leader-vote checked replication page. Default stays 0.1.7.
-- A page is a snapshot under the shared owner lock, not a network lease.
CREATE FUNCTION public.broker_raft_replication_page(k text,e bigint,q jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE STRICT SECURITY DEFINER SET search_path=pg_catalog,broker AS $$
DECLARE first_idx numeric; last_idx numeric; actual_vote jsonb; actual_id jsonb;
BEGIN
 IF octet_length(q::text)>4096 OR jsonb_typeof(q->'vote') IS DISTINCT FROM 'object'
    OR q->'vote'->'committed' IS DISTINCT FROM 'true'::jsonb THEN
  RAISE EXCEPTION 'Invalid replication page request' USING ERRCODE='22023';
 END IF;
 first_idx := broker.raft_log_index(q->'start');
 last_idx := broker.raft_log_index(q->'end');
 IF first_idx>last_idx THEN
  RAISE EXCEPTION 'Invalid replication page interval' USING ERRCODE='22023';
 END IF;
 SELECT vote INTO actual_vote FROM broker.raft_votes
  WHERE group_key=k AND writer_epoch=e FOR SHARE;
 IF NOT FOUND THEN RAISE EXCEPTION 'Replication reader fenced' USING ERRCODE='55000'; END IF;
 IF actual_vote IS DISTINCT FROM q->'vote' THEN
  RETURN jsonb_build_object('vote',actual_vote,'entries','[]'::jsonb);
 END IF;
 SELECT entry->'log_id' INTO actual_id FROM broker.raft_log_entries
  WHERE group_key=k AND log_index=last_idx;
 IF actual_id IS DISTINCT FROM q->'end' THEN
  RAISE EXCEPTION 'Replication endpoint unavailable or changed' USING ERRCODE='55000';
 END IF;
 PERFORM 1 FROM broker.raft_log_entries WHERE group_key=k AND log_index=first_idx;
 IF NOT FOUND THEN RAISE EXCEPTION 'Replication page prefix unavailable' USING ERRCODE='55000'; END IF;
 -- Vote changes, append, truncate, purge and ownership replacement all take the
 -- same row exclusively. No lock or pool slot needs to survive network waiting.
 RETURN jsonb_build_object('vote',actual_vote,
  'entries',public.broker_raft_log_read(k,e,q,false));
END $$;
REVOKE ALL ON FUNCTION public.broker_raft_replication_page(text,bigint,jsonb) FROM PUBLIC;
-- Inherited default privileges must not expose internal replica APIs.
DO $$
DECLARE grantee_name text;
BEGIN
 FOR grantee_name IN
  SELECT DISTINCT r.rolname FROM pg_proc p,
   LATERAL aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) a
   JOIN pg_roles r ON r.oid=a.grantee
  WHERE p.oid='public.broker_raft_replication_page(text,bigint,jsonb)'::regprocedure
    AND a.grantee<>p.proowner
 LOOP
  EXECUTE format('REVOKE ALL ON FUNCTION public.broker_raft_replication_page(text,bigint,jsonb) FROM %I',grantee_name);
 END LOOP;
END $$;
