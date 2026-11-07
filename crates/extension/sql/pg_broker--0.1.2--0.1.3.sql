-- Execute bounded multi-partition reads inside the extension, avoiding a
-- PostgreSQL -> Rust -> SPI conversion for each partition, including empty ones.
CREATE FUNCTION broker.fetch_many(q jsonb) RETURNS jsonb
LANGUAGE plpgsql SET search_path=pg_catalog,broker AS $$
DECLARE result jsonb; budget integer;
BEGIN
 IF jsonb_typeof(q->'requests') IS DISTINCT FROM 'array' THEN RETURN '{"error":42}'; END IF;
 IF jsonb_array_length(q->'requests')>4096 OR EXISTS(
  SELECT 1 FROM jsonb_array_elements(q->'requests') item WHERE jsonb_typeof(item)<>'object'
 ) THEN RETURN '{"error":42}'; END IF;
 budget=least(greatest(coalesce((q->>'max')::integer,4194304),1),4194304);
 WITH RECURSIVE fetched(ord, used, response) AS (
  VALUES (0,0,NULL::jsonb)
  UNION ALL
  SELECT f.ord+1, f.used+coalesce((r.value->>'size')::integer,0), r.value
  FROM fetched f CROSS JOIN LATERAL (
   SELECT broker.dispatch((q->'requests'->f.ord) || jsonb_build_object(
    'op','fetch', 'allow_first',f.used=0,
    'max',least(greatest(coalesce((q->'requests'->f.ord->>'max')::integer,1048576),0),
                1048576,greatest(budget-f.used,0))
   )) AS value
  ) r
  WHERE f.ord<jsonb_array_length(q->'requests')
 )
 SELECT coalesce(jsonb_agg(response ORDER BY ord),'[]'::jsonb) INTO result
 FROM fetched WHERE ord>0;
 RETURN result;
END $$;
REVOKE ALL ON FUNCTION broker.fetch_many(jsonb) FROM PUBLIC;

CREATE OR REPLACE FUNCTION broker.dispatch(q jsonb) RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog,broker AS $$
DECLARE
 op text=q->>'op'; t text=q->>'topic'; p integer=(q->>'partition')::integer;
 g text=q->>'group'; m text=q->>'member'; gen integer=(q->>'generation')::integer;
 gr broker.groups%ROWTYPE; mem broker.members%ROWTYPE; part broker.partitions%ROWTYPE;
 x record; result jsonb; arr jsonb='[]'; off bigint; total integer=0; n integer; lim integer;
BEGIN
 IF op='fetch_many' THEN
  RETURN broker.fetch_many(q);
 ELSIF op='create' THEN
  IF t !~ '^[a-zA-Z0-9_.-]{1,249}$' OR t IN ('.','..') THEN RETURN '{"error":17}'; END IF;
  IF (q->>'partitions')::integer NOT BETWEEN 1 AND 128 THEN RETURN '{"error":37}'; END IF;
  PERFORM pg_advisory_xact_lock(70472102);
  IF EXISTS(SELECT 1 FROM broker.topics WHERE name=t) THEN RETURN '{"error":36}'; END IF;
  IF (SELECT count(*) FROM broker.topics)>=128 THEN RETURN '{"error":42}'; END IF;
  INSERT INTO broker.topics(name,partitions) VALUES(t,(q->>'partitions')::integer) ON CONFLICT DO NOTHING;
  IF NOT FOUND THEN RETURN '{"error":36}'; END IF;
  INSERT INTO broker.partitions(topic,partition) SELECT t,generate_series(0,(q->>'partitions')::integer-1);
  RETURN '{"error":0}';
 ELSIF op='delete' THEN
  DELETE FROM broker.topics WHERE name=t;
  IF NOT FOUND THEN RETURN '{"error":3}'; END IF;
  FOR x IN SELECT * FROM broker.groups WHERE name IN (SELECT group_id FROM broker.members WHERE subscriptions ? t) ORDER BY name FOR UPDATE LOOP
   PERFORM broker.rebalance(x.name);
  END LOOP;
  RETURN '{"error":0}';
 ELSIF op='metadata' THEN
  SELECT coalesce(jsonb_agg(jsonb_build_object('name',name,'partitions',partitions) ORDER BY name),'[]') INTO arr FROM broker.topics;
  RETURN jsonb_build_object('topics',arr,'fetch_many',true);
 ELSIF op IN ('append','fetch','lookup','retain') THEN
  IF op IN ('append','retain') THEN
   SELECT * INTO part FROM broker.partitions WHERE topic=t AND partition=p FOR UPDATE;
  ELSE
   SELECT * INTO part FROM broker.partitions WHERE topic=t AND partition=p;
  END IF;
  IF NOT FOUND THEN RETURN '{"error":3}'; END IF;
  IF op='append' THEN
   off=part.next_offset;
   FOR result IN SELECT value FROM jsonb_array_elements(q->'batches') LOOP
    n=(result->>'count')::integer;
    INSERT INTO broker.batches(topic,partition,base_offset,record_count,data,max_timestamp,timestamps)
     VALUES(t,p,off,n,decode(result->>'data','hex'),(result->>'timestamp')::bigint,result->'timestamps');
    off=off+n;
   END LOOP;
   UPDATE broker.partitions SET next_offset=off WHERE topic=t AND partition=p;
   -- Delivery occurs only after commit; these are hints, never offset state.
   PERFORM pg_notify('pg_broker_records', t || ':' || p::text);
   RETURN jsonb_build_object('error',0,'offset',part.next_offset);
  ELSIF op='lookup' THEN
   off=(q->>'timestamp')::bigint;
   IF off < -2 THEN RETURN '{"error":42}'; END IF;
   IF off=-2 THEN off=part.earliest;
   ELSIF off=-1 THEN off=part.next_offset;
   ELSE
    SELECT b.base_offset+a.ordinality-1 AS found_offset, a.value::text::bigint AS found_timestamp INTO x
     FROM broker.batches b CROSS JOIN LATERAL jsonb_array_elements(b.timestamps) WITH ORDINALITY a
     WHERE b.topic=t AND b.partition=p AND b.max_timestamp>=off AND a.value::text::bigint>=off
     ORDER BY b.base_offset,a.ordinality LIMIT 1;
    IF NOT FOUND THEN RETURN '{"error":0,"offset":-1,"timestamp":-1}'; END IF;
    RETURN jsonb_build_object('error',0,'offset',x.found_offset,'timestamp',x.found_timestamp);
   END IF;
   RETURN jsonb_build_object('error',0,'offset',off,'timestamp',-1);
  ELSIF op='fetch' THEN
   off=(q->>'offset')::bigint;
   IF off<part.earliest OR off>part.next_offset THEN RETURN jsonb_build_object('error',1,'high',part.next_offset); END IF;
   lim=least(greatest((q->>'max')::integer,0),4194304);
   -- One statement snapshot for records plus bounds is provided by the gateway's
   -- REPEATABLE READ transaction. A batch covering off may start before off.
   -- Seek to the batch covering off before scanning forward. Filtering only
   -- on base_offset+record_count otherwise scans every older retained batch.
   FOR x IN SELECT * FROM broker.batches WHERE topic=t AND partition=p
    AND base_offset >= coalesce((SELECT base_offset FROM broker.batches
     WHERE topic=t AND partition=p AND base_offset<=off ORDER BY base_offset DESC LIMIT 1),off)
    AND base_offset+record_count>off ORDER BY base_offset LIMIT 4096 LOOP
    EXIT WHEN (total>0 OR NOT coalesce((q->>'allow_first')::boolean,true)) AND total+octet_length(x.data)>lim;
    EXIT WHEN lim=0;
    arr=arr||jsonb_build_array(jsonb_build_object('offset',x.base_offset,'data',encode(x.data,'hex')));
    total=total+octet_length(x.data);
   END LOOP;
   RETURN jsonb_build_object('error',0,'high',part.next_offset,'earliest',part.earliest,'batches',arr,'size',total);
  ELSE
   IF (q->>'retention_ms')::bigint<0 THEN RETURN '{"error":42}'; END IF;
   -- Delete at most 4096 batches per transaction. Retain a contiguous suffix,
   -- including when the wall clock moves backwards.
   SELECT coalesce(min(base_offset) FILTER (WHERE appended_at>=clock_timestamp()-((q->>'retention_ms')::bigint*interval '1 millisecond')),
                   max(base_offset+record_count),part.next_offset) INTO off
    FROM (SELECT * FROM broker.batches WHERE topic=t AND partition=p ORDER BY base_offset LIMIT 4096) page;
   DELETE FROM broker.batches WHERE topic=t AND partition=p AND base_offset<off;
   UPDATE broker.partitions SET earliest=off WHERE topic=t AND partition=p;
   RETURN jsonb_build_object('error',0,'earliest',off,'high',part.next_offset);
  END IF;
 ELSIF op='retention_scan' THEN
  SELECT coalesce(jsonb_agg(jsonb_build_object('topic',a.topic,'partition',a.partition,'retention_ms',b.retention_ms)),'[]') INTO arr FROM broker.partitions a JOIN broker.topics b ON b.name=a.topic;
  RETURN arr;
 END IF;
 -- The database is the single logical coordinator. Every gateway serializes
 -- transitions on this row, avoiding leases and split-brain ownership.
 INSERT INTO broker.groups(name) VALUES(g) ON CONFLICT DO NOTHING;
 SELECT * INTO gr FROM broker.groups WHERE name=g FOR UPDATE;
 PERFORM broker.expire_members(g);
 SELECT * INTO gr FROM broker.groups WHERE name=g;
 IF op='join' THEN
  IF (q->>'session_ms')::integer NOT BETWEEN 3000 AND 300000 THEN RETURN '{"error":26}'; END IF;
  SELECT * INTO mem FROM broker.members WHERE group_id=g AND member_id=m;
  IF NOT FOUND AND NOT (q->>'new')::boolean THEN RETURN '{"error":25}'; END IF;
  IF NOT FOUND AND (SELECT count(*) FROM broker.members WHERE group_id=g)>=128 THEN RETURN '{"error":23}'; END IF;
  UPDATE broker.groups SET rebalance_ms=greatest(rebalance_ms,least(coalesce((q->>'rebalance_ms')::integer,10000),300000)) WHERE name=g;
  IF gr.state IN ('empty','stable','completing') THEN
   PERFORM broker.rebalance(g); SELECT * INTO gr FROM broker.groups WHERE name=g;
  END IF;
  INSERT INTO broker.members(group_id,member_id,metadata,subscriptions,joined,expires,session_ms)
   VALUES(g,m,q->>'metadata',q->'subscriptions',gr.generation,clock_timestamp()+((q->>'session_ms')::integer*interval '1 millisecond'),(q->>'session_ms')::integer)
   ON CONFLICT(group_id,member_id) DO UPDATE SET metadata=excluded.metadata,subscriptions=excluded.subscriptions,joined=excluded.joined,expires=excluded.expires,session_ms=excluded.session_ms;
  RETURN jsonb_build_object('error',0,'generation',gr.generation);
 ELSIF op='join_poll' THEN
  IF gen<>gr.generation THEN RETURN '{"error":27}'; END IF;
  UPDATE broker.members SET expires=clock_timestamp()+session_ms*interval '1 millisecond' WHERE group_id=g AND member_id=m AND joined=gen;
  IF NOT FOUND THEN RETURN '{"error":25}'; END IF;
  IF gr.state='joining' THEN
   IF clock_timestamp()>=gr.deadline THEN DELETE FROM broker.members WHERE group_id=g AND joined<>gen; END IF;
   IF EXISTS(SELECT 1 FROM broker.members WHERE group_id=g AND joined<>gen) THEN RETURN '{"wait":true}'; END IF;
   SELECT min(member_id) INTO gr.leader FROM broker.members WHERE group_id=g;
   UPDATE broker.groups SET state='completing',leader=gr.leader WHERE name=g;
  END IF;
  SELECT coalesce(jsonb_agg(jsonb_build_object('member',member_id,'metadata',metadata) ORDER BY member_id),'[]') INTO arr FROM broker.members WHERE group_id=g;
  RETURN jsonb_build_object('error',0,'generation',gen,'leader',gr.leader,'members',arr);
 ELSIF op='offset_fetch' THEN
  IF NOT EXISTS(SELECT 1 FROM broker.partitions WHERE topic=t AND partition=p) THEN RETURN '{"error":3}'; END IF;
  SELECT committed,metadata INTO x FROM broker.offsets WHERE group_id=g AND topic=t AND partition=p;
  IF NOT FOUND THEN RETURN '{"error":0,"offset":-1,"metadata":""}'; END IF;
  RETURN jsonb_build_object('error',0,'offset',x.committed,'metadata',x.metadata);
 ELSIF op='leave' THEN
  DELETE FROM broker.members WHERE group_id=g AND member_id=m;
  IF NOT FOUND THEN RETURN '{"error":25}'; END IF;
  PERFORM broker.rebalance(g); RETURN '{"error":0}';
 END IF;
 IF op='commit' AND gen=-1 AND m='' THEN
  IF EXISTS(SELECT 1 FROM broker.members WHERE group_id=g) THEN RETURN '{"error":22}'; END IF;
 ELSE
  SELECT * INTO mem FROM broker.members WHERE group_id=g AND member_id=m;
  IF NOT FOUND THEN RETURN '{"error":25}'; END IF;
  IF op='heartbeat' AND gr.state='joining' THEN RETURN '{"error":27}'; END IF;
  IF gen<>gr.generation THEN RETURN '{"error":22}'; END IF;
  IF gr.state='joining' THEN RETURN '{"error":27}'; END IF;
 END IF;
 IF op='heartbeat' THEN
  IF gr.state<>'stable' THEN RETURN '{"error":27}'; END IF;
  UPDATE broker.members SET expires=clock_timestamp()+session_ms*interval '1 millisecond' WHERE group_id=g AND member_id=m;
  RETURN '{"error":0}';
 ELSIF op='sync' THEN
  IF jsonb_array_length(q->'assignments')>0 THEN
   IF m<>gr.leader OR gr.state NOT IN ('completing','stable') THEN RETURN '{"error":27}'; END IF;
   IF gr.state='stable' AND EXISTS(SELECT 1 FROM jsonb_array_elements(q->'assignments') a
    JOIN broker.members b ON b.group_id=g AND b.member_id=a->>'member' WHERE b.assignment<>a->>'assignment') THEN RETURN '{"error":27}'; END IF;
   IF (SELECT count(*) FROM broker.members WHERE group_id=g)<>jsonb_array_length(q->'assignments')
     OR (SELECT count(DISTINCT a->>'member') FROM jsonb_array_elements(q->'assignments') a)<>jsonb_array_length(q->'assignments')
     OR EXISTS(SELECT 1 FROM jsonb_array_elements(q->'assignments') a WHERE NOT EXISTS(SELECT 1 FROM broker.members WHERE group_id=g AND member_id=a->>'member')) THEN RETURN '{"error":23}'; END IF;
   IF EXISTS(
    SELECT 1 FROM jsonb_array_elements(q->'assignments') a,
     LATERAL jsonb_array_elements(a->'owned') o
    WHERE NOT EXISTS(SELECT 1 FROM broker.partitions WHERE topic=o->>0 AND partition=(o->>1)::integer)
       OR NOT EXISTS(SELECT 1 FROM broker.members WHERE group_id=g AND member_id=a->>'member' AND subscriptions ? (o->>0))
   ) THEN RETURN '{"error":23}'; END IF;
   FOR result IN SELECT value FROM jsonb_array_elements(q->'assignments') LOOP
    UPDATE broker.members SET assignment=result->>'assignment',owned=result->'owned' WHERE group_id=g AND member_id=result->>'member';
   END LOOP;
   UPDATE broker.groups SET state='stable' WHERE name=g;
  END IF;
  -- text is returned below directly; do not cast the encoded bytes as JSON.
  SELECT assignment INTO mem.assignment FROM broker.members WHERE group_id=g AND member_id=m;
  IF mem.assignment IS NULL THEN RETURN '{"wait":true}'; END IF;
  RETURN jsonb_build_object('error',0,'assignment',mem.assignment);
 ELSIF op='commit' THEN
  IF gen>=0 AND gr.state<>'stable' THEN RETURN '{"error":27}'; END IF;
  IF gen>=0 AND NOT mem.owned @> jsonb_build_array(jsonb_build_array(t,p)) THEN RETURN '{"error":27}'; END IF;
  IF NOT EXISTS(SELECT 1 FROM broker.partitions WHERE topic=t AND partition=p) THEN RETURN '{"error":3}'; END IF;
  IF (q->>'offset')::bigint<0 THEN RETURN '{"error":1}'; END IF;
  INSERT INTO broker.offsets VALUES(g,t,p,(q->>'offset')::bigint,coalesce(q->>'metadata',''))
   ON CONFLICT(group_id,topic,partition) DO UPDATE SET committed=excluded.committed,metadata=excluded.metadata;
  RETURN '{"error":0}';
 END IF;
 RETURN '{"error":42}';
END $$;

-- Read-only entry point: dispatch operations are forced to fetch in fetch_many.
-- Avoid serializing record bytes through Rust/SPI on the fetch path. Grant only
-- to the gateway role, just as for broker_call; older grants use the SPI fallback.
CREATE FUNCTION public.broker_fetch_many(request jsonb) RETURNS jsonb
LANGUAGE sql VOLATILE STRICT PARALLEL UNSAFE SECURITY DEFINER
SET search_path=pg_catalog,broker
AS 'SELECT broker.fetch_many(request)';
REVOKE ALL ON FUNCTION public.broker_fetch_many(jsonb) FROM PUBLIC;
