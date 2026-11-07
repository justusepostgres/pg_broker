-- Opt-in exclusive partition application. Default deployment remains 0.1.7.
-- Context rows exist only inside a successful apply call and are deleted before
-- it returns. They are not GUCs: ordinary callers cannot forge authorization.
CREATE TABLE broker.raft_apply_context (
 group_key text PRIMARY KEY REFERENCES broker.raft_partition_state,
 transaction_id xid8 NOT NULL
);
REVOKE ALL ON broker.raft_apply_context FROM PUBLIC;

CREATE FUNCTION broker.raft_require_application(t text,p integer) RETURNS void
LANGUAGE plpgsql SET search_path=pg_catalog,broker AS $$
DECLARE k text;
BEGIN
 -- Binding updates this row, fencing writers using an older RR snapshot.
 PERFORM 1 FROM broker.partitions WHERE topic=t AND partition=p FOR UPDATE;
 SELECT group_key INTO k FROM broker.raft_partition_state WHERE topic=t AND partition=p;
 IF FOUND AND NOT EXISTS(SELECT 1 FROM broker.raft_apply_context
                         WHERE group_key=k AND transaction_id=pg_current_xact_id()) THEN
  RAISE EXCEPTION 'Bound partition requires committed Raft application' USING ERRCODE='55000';
 END IF;
END $$;
REVOKE ALL ON FUNCTION broker.raft_require_application(text,integer) FROM PUBLIC;

CREATE FUNCTION broker.raft_guard_batches() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog,broker AS $$
BEGIN
 IF TG_OP<>'INSERT' THEN PERFORM broker.raft_require_application(OLD.topic,OLD.partition); END IF;
 IF TG_OP<>'DELETE' THEN PERFORM broker.raft_require_application(NEW.topic,NEW.partition); RETURN NEW; END IF;
 RETURN OLD;
END $$;
REVOKE ALL ON FUNCTION broker.raft_guard_batches() FROM PUBLIC;
CREATE TRIGGER raft_guard_batches BEFORE INSERT OR UPDATE OR DELETE ON broker.batches
FOR EACH ROW EXECUTE FUNCTION broker.raft_guard_batches();

CREATE FUNCTION broker.raft_guard_partition() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog,broker AS $$
BEGIN
 IF TG_OP='DELETE' THEN
  IF EXISTS(SELECT 1 FROM broker.raft_partition_state WHERE topic=OLD.topic AND partition=OLD.partition) THEN
   RAISE EXCEPTION 'Cannot delete a bound partition' USING ERRCODE='55000';
  END IF;
  RETURN OLD;
 END IF;
 IF NEW IS DISTINCT FROM OLD THEN
  PERFORM broker.raft_require_application(OLD.topic,OLD.partition);
  IF (NEW.topic,NEW.partition) IS DISTINCT FROM (OLD.topic,OLD.partition) THEN
   PERFORM broker.raft_require_application(NEW.topic,NEW.partition);
  END IF;
 END IF;
 RETURN NEW;
END $$;
REVOKE ALL ON FUNCTION broker.raft_guard_partition() FROM PUBLIC;
CREATE TRIGGER raft_guard_partition BEFORE UPDATE OR DELETE ON broker.partitions
FOR EACH ROW EXECUTE FUNCTION broker.raft_guard_partition();

-- Copy implementation into a new private function; retain the public wrapper's
-- OID and grants rather than accidentally transferring grants to the helper.
DO $copy$ BEGIN
 EXECUTE replace(pg_get_functiondef('public.broker_raft_partition_bind(text,bigint,text,uuid,integer)'::regprocedure),
                 'FUNCTION public.broker_raft_partition_bind(', 'FUNCTION broker.raft_partition_bind_unfenced(');
END $copy$;
REVOKE ALL ON FUNCTION broker.raft_partition_bind_unfenced(text,bigint,text,uuid,integer) FROM PUBLIC;
CREATE OR REPLACE FUNCTION public.broker_raft_partition_bind(k text,e bigint,t text,tid uuid,p integer)
RETURNS jsonb LANGUAGE plpgsql VOLATILE STRICT SECURITY DEFINER SET search_path=pg_catalog,broker AS $$
BEGIN
 PERFORM 1 FROM broker.raft_votes WHERE group_key=k AND writer_epoch=e FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'Raft application owner fenced' USING ERRCODE='55000'; END IF;
 PERFORM 1 FROM broker.topics WHERE name=t AND topic_id=tid FOR KEY SHARE;
 IF NOT FOUND THEN RAISE EXCEPTION 'Replica topic identity mismatch' USING ERRCODE='55000'; END IF;
 UPDATE broker.partitions SET next_offset=next_offset WHERE topic=t AND partition=p;
 RETURN broker.raft_partition_bind_unfenced(k,e,t,tid,p);
END $$;
REVOKE ALL ON FUNCTION public.broker_raft_partition_bind(text,bigint,text,uuid,integer) FROM PUBLIC;

-- Copy implementation into a new private function; retain the public wrapper's
-- OID and grants rather than accidentally transferring grants to the helper.
DO $copy$ BEGIN
 EXECUTE replace(pg_get_functiondef('public.broker_raft_partition_apply(text,bigint,jsonb)'::regprocedure),
                 'FUNCTION public.broker_raft_partition_apply(', 'FUNCTION broker.raft_partition_apply_inner(');
END $copy$;
REVOKE ALL ON FUNCTION broker.raft_partition_apply_inner(text,bigint,jsonb) FROM PUBLIC;
CREATE OR REPLACE FUNCTION public.broker_raft_partition_apply(k text,e bigint,id jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE STRICT SECURITY DEFINER SET search_path=pg_catalog,broker AS $$
DECLARE result jsonb;
BEGIN
 PERFORM 1 FROM broker.raft_votes WHERE group_key=k AND writer_epoch=e FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'Raft application owner fenced' USING ERRCODE='55000'; END IF;
 INSERT INTO broker.raft_apply_context VALUES(k,pg_current_xact_id());
 result := broker.raft_partition_apply_inner(k,e,id);
 DELETE FROM broker.raft_apply_context WHERE group_key=k;
 RETURN result;
END $$;
REVOKE ALL ON FUNCTION public.broker_raft_partition_apply(text,bigint,jsonb) FROM PUBLIC;

-- ALTER DEFAULT PRIVILEGES may grant newly created objects to application roles.
-- Explicitly remove those grants from private authorization state and helpers.
DO $private$
DECLARE a record; target regprocedure; principal text;
BEGIN
 FOR a IN SELECT DISTINCT x.grantee FROM pg_class c,
   LATERAL aclexplode(coalesce(c.relacl,acldefault('r',c.relowner))) x
   WHERE c.oid='broker.raft_apply_context'::regclass AND x.grantee<>c.relowner LOOP
  principal := CASE WHEN a.grantee=0 THEN 'PUBLIC' ELSE quote_ident(pg_get_userbyid(a.grantee)) END;
  EXECUTE 'REVOKE ALL ON TABLE broker.raft_apply_context FROM '||principal||' CASCADE';
 END LOOP;
 FOREACH target IN ARRAY ARRAY[
   'broker.raft_require_application(text,integer)'::regprocedure,
   'broker.raft_guard_batches()'::regprocedure,
   'broker.raft_guard_partition()'::regprocedure,
   'broker.raft_partition_bind_unfenced(text,bigint,text,uuid,integer)'::regprocedure,
   'broker.raft_partition_apply_inner(text,bigint,jsonb)'::regprocedure] LOOP
  FOR a IN SELECT DISTINCT x.grantee FROM pg_proc p,
    LATERAL aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) x
    WHERE p.oid=target AND x.grantee<>p.proowner LOOP
   principal := CASE WHEN a.grantee=0 THEN 'PUBLIC' ELSE quote_ident(pg_get_userbyid(a.grantee)) END;
   EXECUTE format('REVOKE ALL ON FUNCTION %s FROM %s CASCADE',target,principal);
  END LOOP;
 END LOOP;
END $private$;
