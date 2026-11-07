-- Opt-in bulk offset commits. One durable group lock/check per Kafka request;
-- partition validation and duplicate updates remain in original wire order.
CREATE FUNCTION broker.commit_many(q jsonb)
RETURNS TABLE(request_index integer, error integer)
LANGUAGE plpgsql VOLATILE STRICT PARALLEL UNSAFE
SET search_path=pg_catalog,broker AS $$
DECLARE requests jsonb=q->'requests'; item record; g text; m text; gen integer;
 gr broker.groups%ROWTYPE; mem broker.members%ROWTYPE; group_error integer=0;
 t text; p integer; off bigint;
BEGIN
 IF jsonb_typeof(requests) IS DISTINCT FROM 'array' THEN
  RAISE EXCEPTION 'commit requests must be an array' USING ERRCODE='22023';
 END IF;
 IF jsonb_array_length(requests)>4096 OR octet_length(q::text)>67108864 THEN
  RAISE EXCEPTION 'commit request limit' USING ERRCODE='54000';
 END IF;
 IF jsonb_array_length(requests)=0 THEN RETURN; END IF;
 g=requests->0->>'group'; m=requests->0->>'member'; gen=(requests->0->>'generation')::integer;
 IF g IS NULL OR m IS NULL OR gen IS NULL OR EXISTS(
  SELECT 1 FROM jsonb_array_elements(requests) r WHERE jsonb_typeof(r)<>'object'
   OR r->>'op' IS DISTINCT FROM 'commit' OR r->>'group' IS DISTINCT FROM g
   OR r->>'member' IS DISTINCT FROM m OR (r->>'generation')::integer IS DISTINCT FROM gen
 ) THEN RAISE EXCEPTION 'commit requests must share one group/member/generation' USING ERRCODE='22023'; END IF;
 INSERT INTO broker.groups(name) VALUES(g) ON CONFLICT DO NOTHING;
 SELECT * INTO gr FROM broker.groups WHERE name=g FOR UPDATE;
 PERFORM broker.expire_members(g);
 SELECT * INTO gr FROM broker.groups WHERE name=g;
 IF gen=-1 AND m='' THEN
  IF EXISTS(SELECT 1 FROM broker.members WHERE group_id=g) THEN group_error=22; END IF;
 ELSE
  SELECT * INTO mem FROM broker.members WHERE group_id=g AND member_id=m;
  IF NOT FOUND THEN group_error=25;
  ELSIF gen<>gr.generation THEN group_error=22;
  ELSIF gr.state='joining' THEN group_error=27;
  END IF;
 END IF;
 IF group_error=0 AND gen>=0 AND gr.state<>'stable' THEN group_error=27; END IF;
 FOR item IN SELECT value,ordinality FROM jsonb_array_elements(requests) WITH ORDINALITY ORDER BY ordinality LOOP
  request_index=(item.ordinality-1)::integer; error=group_error;
  IF error<>0 THEN RETURN NEXT; CONTINUE; END IF;
  t=item.value->>'topic'; p=(item.value->>'partition')::integer; off=(item.value->>'offset')::bigint;
  IF gen>=0 AND NOT mem.owned @> jsonb_build_array(jsonb_build_array(t,p)) THEN error=27;
  ELSIF NOT EXISTS(SELECT 1 FROM broker.partitions WHERE topic=t AND partition=p) THEN error=3;
  ELSIF off<0 THEN error=1;
  ELSE
   INSERT INTO broker.offsets VALUES(g,t,p,off,coalesce(item.value->>'metadata',''))
    ON CONFLICT(group_id,topic,partition) DO UPDATE SET committed=excluded.committed,metadata=excluded.metadata;
  END IF;
  RETURN NEXT;
 END LOOP;
END $$;
REVOKE ALL ON FUNCTION broker.commit_many(jsonb) FROM PUBLIC;
CREATE FUNCTION public.broker_commit_many(q jsonb) RETURNS jsonb
LANGUAGE sql VOLATILE STRICT PARALLEL UNSAFE SECURITY DEFINER
SET search_path=pg_catalog,broker AS $$
 SELECT coalesce(jsonb_agg(jsonb_build_object('error',r.error) ORDER BY r.request_index),'[]'::jsonb)
 FROM broker.commit_many(q) r
$$;
REVOKE ALL ON FUNCTION public.broker_commit_many(jsonb) FROM PUBLIC;
-- Do not inherit default application grants on the private helper.
DO $private$
DECLARE a record; principal text;
BEGIN
 FOR a IN SELECT DISTINCT x.grantee FROM pg_proc p,
  LATERAL aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) x
  WHERE p.oid='broker.commit_many(jsonb)'::regprocedure AND x.grantee<>p.proowner LOOP
  principal=CASE WHEN a.grantee=0 THEN 'PUBLIC' ELSE quote_ident(pg_get_userbyid(a.grantee)) END;
  EXECUTE 'REVOKE ALL ON FUNCTION broker.commit_many(jsonb) FROM '||principal||' CASCADE';
 END LOOP;
END $private$;
