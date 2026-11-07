-- Experimental committed application pages, independent of a live Raft engine.
-- Each page holds the owner lock only for its database transaction.
CREATE FUNCTION public.broker_raft_committed_page(k text,e bigint,q jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE STRICT SECURITY DEFINER SET search_path=pg_catalog,broker AS $$
DECLARE first_idx numeric; last_idx numeric; committed_id jsonb; actual_id jsonb;
BEGIN
 IF octet_length(q::text)>4096 THEN
  RAISE EXCEPTION 'Invalid committed page bounds' USING ERRCODE='22023';
 END IF;
 first_idx := broker.raft_log_index(q->'start');
 last_idx := broker.raft_log_index(q->'end');
 IF first_idx>last_idx THEN
  RAISE EXCEPTION 'Invalid committed page interval' USING ERRCODE='22023';
 END IF;
 PERFORM 1 FROM broker.raft_votes WHERE group_key=k AND writer_epoch=e FOR SHARE;
 IF NOT FOUND THEN RAISE EXCEPTION 'Committed reader fenced' USING ERRCODE='55000'; END IF;
 SELECT committed INTO committed_id FROM broker.raft_log_state WHERE group_key=k;
 IF committed_id IS NULL OR broker.raft_log_index(committed_id)<last_idx THEN
  RAISE EXCEPTION 'Requested range is not committed' USING ERRCODE='55000';
 END IF;
 SELECT entry->'log_id' INTO actual_id FROM broker.raft_log_entries
  WHERE group_key=k AND log_index=last_idx;
 IF actual_id IS DISTINCT FROM q->'end' THEN
  RAISE EXCEPTION 'Committed endpoint unavailable or changed' USING ERRCODE='55000';
 END IF;
 PERFORM 1 FROM broker.raft_log_entries WHERE group_key=k AND log_index=first_idx;
 IF NOT FOUND THEN RAISE EXCEPTION 'Committed page prefix unavailable' USING ERRCODE='55000'; END IF;
 -- The existing primitive enforces 256 entries / 8 MiB before aggregation.
 -- All supported journal mutations require the same owner row's exclusive lock.
 RETURN public.broker_raft_log_read(k,e,q,false);
END $$;
REVOKE ALL ON FUNCTION public.broker_raft_committed_page(text,bigint,jsonb) FROM PUBLIC;
-- Inherited default privileges must not expose internal replica APIs.
DO $$
DECLARE grantee_name text;
BEGIN
 FOR grantee_name IN
  SELECT DISTINCT r.rolname FROM pg_proc p,
   LATERAL aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) a
   JOIN pg_roles r ON r.oid=a.grantee
  WHERE p.oid='public.broker_raft_committed_page(text,bigint,jsonb)'::regprocedure
    AND a.grantee<>p.proowner
 LOOP
  EXECUTE format('REVOKE ALL ON FUNCTION public.broker_raft_committed_page(text,bigint,jsonb) FROM %I',grantee_name);
 END LOOP;
END $$;
