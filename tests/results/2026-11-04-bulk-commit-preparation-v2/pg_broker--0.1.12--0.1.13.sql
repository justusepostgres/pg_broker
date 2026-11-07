-- Durable paged local snapshot capture. Export/install/networking are not supplied.
CREATE TABLE broker.raft_snapshots (
 snapshot_id uuid PRIMARY KEY DEFAULT pg_catalog.gen_random_uuid(),
 group_key text NOT NULL REFERENCES broker.raft_partition_state ON DELETE CASCADE,
 manifest jsonb NOT NULL,
 topic text NOT NULL,
 partition integer NOT NULL,
 earliest bigint NOT NULL,
 end_offset bigint NOT NULL,
 cursor_offset bigint NOT NULL,
 state text NOT NULL CHECK(state IN ('capturing','complete','releasing')),
 CHECK(earliest<=cursor_offset AND cursor_offset<=end_offset)
);
CREATE UNIQUE INDEX raft_snapshot_one_capture ON broker.raft_snapshots(group_key) WHERE state='capturing';
CREATE TABLE broker.raft_snapshot_batches (
 snapshot_id uuid REFERENCES broker.raft_snapshots ON DELETE CASCADE,
 base_offset bigint NOT NULL,
 record_count integer NOT NULL,
 data bytea NOT NULL,
 max_timestamp bigint NOT NULL,
 timestamps jsonb NOT NULL,
 appended_at timestamptz NOT NULL,
 PRIMARY KEY(snapshot_id,base_offset)
);
SELECT pg_catalog.pg_extension_config_dump('broker.raft_snapshots','');
SELECT pg_catalog.pg_extension_config_dump('broker.raft_snapshot_batches','');

-- Lock the partition before testing pins. Snapshot begin also updates that row:
-- old REPEATABLE READ writers fail serialization rather than miss a new pin.
CREATE FUNCTION broker.raft_snapshot_preserve_source() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog,broker AS $$
BEGIN
 PERFORM 1 FROM broker.partitions WHERE topic=OLD.topic AND partition=OLD.partition FOR UPDATE;
 IF EXISTS(SELECT 1 FROM broker.raft_snapshots WHERE topic=OLD.topic AND partition=OLD.partition
           AND state='capturing' AND OLD.base_offset<end_offset
           AND OLD.base_offset+OLD.record_count>earliest) THEN
  RAISE EXCEPTION 'Snapshot source range is pinned' USING ERRCODE='55000';
 END IF;
 IF TG_OP='DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF;
END $$;
REVOKE ALL ON FUNCTION broker.raft_snapshot_preserve_source() FROM PUBLIC;
CREATE TRIGGER raft_snapshot_preserve_source BEFORE UPDATE OR DELETE ON broker.batches
FOR EACH ROW EXECUTE FUNCTION broker.raft_snapshot_preserve_source();

CREATE FUNCTION public.broker_raft_snapshot_begin(k text,e bigint) RETURNS jsonb
LANGUAGE plpgsql VOLATILE STRICT SECURITY DEFINER SET search_path=pg_catalog,broker AS $$
DECLARE s broker.raft_partition_state; p broker.partitions; t broker.topics; snap broker.raft_snapshots;
BEGIN
 PERFORM 1 FROM broker.raft_votes WHERE group_key=k AND writer_epoch=e FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'Snapshot owner fenced' USING ERRCODE='55000'; END IF;
 SELECT * INTO s FROM broker.raft_partition_state WHERE group_key=k;
 IF NOT FOUND OR s.applied IS NULL THEN RAISE EXCEPTION 'Snapshot requires applied state' USING ERRCODE='55000'; END IF;
 SELECT * INTO snap FROM broker.raft_snapshots WHERE group_key=k AND state='capturing';
 IF FOUND THEN RETURN to_jsonb(snap); END IF;
 IF (SELECT count(*) FROM broker.raft_snapshots WHERE group_key=k)>=2 THEN
  RAISE EXCEPTION 'Snapshot count limit; release an obsolete snapshot' USING ERRCODE='54000';
 END IF;
 SELECT * INTO t FROM broker.topics WHERE name=s.topic AND topic_id=s.topic_id FOR KEY SHARE;
 IF NOT FOUND THEN RAISE EXCEPTION 'Snapshot topic identity mismatch' USING ERRCODE='55000'; END IF;
 UPDATE broker.partitions SET next_offset=next_offset WHERE topic=s.topic AND partition=s.partition RETURNING * INTO p;
 IF NOT FOUND THEN RAISE EXCEPTION 'Snapshot partition missing' USING ERRCODE='55000'; END IF;
 INSERT INTO broker.raft_snapshots(group_key,manifest,topic,partition,earliest,end_offset,cursor_offset,state)
 VALUES(k,jsonb_build_object('format',1,'application',to_jsonb(s),'topic',to_jsonb(t)),
        s.topic,s.partition,p.earliest,p.next_offset,p.earliest,
        CASE WHEN p.earliest=p.next_offset THEN 'complete' ELSE 'capturing' END)
 RETURNING * INTO snap;
 RETURN to_jsonb(snap);
END $$;
REVOKE ALL ON FUNCTION public.broker_raft_snapshot_begin(text,bigint) FROM PUBLIC;

CREATE FUNCTION public.broker_raft_snapshot_copy(k text,e bigint,sid uuid) RETURNS jsonb
LANGUAGE plpgsql VOLATILE STRICT SECURITY DEFINER SET search_path=pg_catalog,broker AS $$
DECLARE snap broker.raft_snapshots; b broker.batches; item record; used integer:=0; amount integer;
BEGIN
 PERFORM 1 FROM broker.raft_votes WHERE group_key=k AND writer_epoch=e FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'Snapshot owner fenced' USING ERRCODE='55000'; END IF;
 SELECT * INTO snap FROM broker.raft_snapshots WHERE group_key=k AND snapshot_id=sid FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'Snapshot not found' USING ERRCODE='55000'; END IF;
 IF snap.state='complete' THEN RETURN to_jsonb(snap); END IF;
 IF snap.state<>'capturing' THEN RAISE EXCEPTION 'Snapshot is being released' USING ERRCODE='55000'; END IF;
 FOR item IN SELECT base_offset,octet_length(data)+octet_length(timestamps::text)+256 AS amount FROM broker.batches WHERE topic=snap.topic AND partition=snap.partition
          AND base_offset>=snap.cursor_offset AND base_offset<snap.end_offset ORDER BY base_offset LIMIT 128 LOOP
  amount:=item.amount;
  IF used+amount>8388608 THEN EXIT; END IF;
  SELECT * INTO b FROM broker.batches WHERE topic=snap.topic AND partition=snap.partition AND base_offset=item.base_offset;
  IF b.base_offset<>snap.cursor_offset OR b.base_offset+b.record_count>snap.end_offset THEN
   RAISE EXCEPTION 'Snapshot source is not contiguous' USING ERRCODE='55000';
  END IF;
  INSERT INTO broker.raft_snapshot_batches VALUES(sid,b.base_offset,b.record_count,b.data,b.max_timestamp,b.timestamps,b.appended_at);
  snap.cursor_offset:=b.base_offset+b.record_count;
  used:=used+amount;
 END LOOP;
 IF used=0 THEN RAISE EXCEPTION 'Snapshot source missing or exceeds page budget' USING ERRCODE='55000'; END IF;
 UPDATE broker.raft_snapshots SET cursor_offset=snap.cursor_offset,
   state=CASE WHEN snap.cursor_offset=end_offset THEN 'complete' ELSE 'capturing' END
  WHERE snapshot_id=sid RETURNING * INTO snap;
 RETURN to_jsonb(snap);
END $$;
REVOKE ALL ON FUNCTION public.broker_raft_snapshot_copy(text,bigint,uuid) FROM PUBLIC;

-- Explicit, resumable release/abort; never accumulate more than two manifests.
CREATE FUNCTION public.broker_raft_snapshot_release(k text,e bigint,sid uuid) RETURNS jsonb
LANGUAGE plpgsql VOLATILE STRICT SECURITY DEFINER SET search_path=pg_catalog,broker AS $$
DECLARE snap broker.raft_snapshots;
BEGIN
 PERFORM 1 FROM broker.raft_votes WHERE group_key=k AND writer_epoch=e FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'Snapshot owner fenced' USING ERRCODE='55000'; END IF;
 SELECT * INTO snap FROM broker.raft_snapshots WHERE group_key=k AND snapshot_id=sid FOR UPDATE;
 IF NOT FOUND THEN RETURN '{"released":true}'; END IF;
 UPDATE broker.raft_snapshots SET state='releasing' WHERE snapshot_id=sid;
 DELETE FROM broker.raft_snapshot_batches WHERE snapshot_id=sid AND base_offset IN
  (SELECT base_offset FROM broker.raft_snapshot_batches WHERE snapshot_id=sid ORDER BY base_offset LIMIT 128);
 IF EXISTS(SELECT 1 FROM broker.raft_snapshot_batches WHERE snapshot_id=sid) THEN RETURN '{"released":false}'; END IF;
 DELETE FROM broker.raft_snapshots WHERE snapshot_id=sid;
 RETURN '{"released":true}';
END $$;
REVOKE ALL ON FUNCTION public.broker_raft_snapshot_release(text,bigint,uuid) FROM PUBLIC;
