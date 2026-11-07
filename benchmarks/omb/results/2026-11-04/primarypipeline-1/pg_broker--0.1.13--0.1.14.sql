-- Bounded reads of immutable completed images; no network or install protocol.
CREATE FUNCTION public.broker_raft_snapshot_read(k text,e bigint,sid uuid,start_offset bigint)
RETURNS jsonb LANGUAGE plpgsql VOLATILE STRICT SECURITY DEFINER
SET search_path=pg_catalog,broker AS $$
DECLARE snap broker.raft_snapshots; item record; used integer:=0;
 next_offset bigint:=start_offset; batches jsonb; result jsonb;
BEGIN
 PERFORM 1 FROM broker.raft_votes WHERE group_key=k AND writer_epoch=e FOR SHARE;
 IF NOT FOUND THEN RAISE EXCEPTION 'Snapshot reader fenced' USING ERRCODE='55000'; END IF;
 SELECT * INTO snap FROM broker.raft_snapshots WHERE group_key=k AND snapshot_id=sid;
 IF NOT FOUND OR snap.state<>'complete' THEN RAISE EXCEPTION 'Completed snapshot required' USING ERRCODE='55000'; END IF;
 IF start_offset<snap.earliest OR start_offset>snap.end_offset THEN
  RAISE EXCEPTION 'Snapshot cursor outside retained interval' USING ERRCODE='22023';
 END IF;
 -- Scan only metadata first. At most 2 MiB of raw data/metadata becomes at most
 -- approximately 4 MiB after hex encoding, leaving room for the manifest.
 FOR item IN SELECT base_offset,record_count,octet_length(data)+octet_length(timestamps::text)+256 AS bytes
  FROM broker.raft_snapshot_batches WHERE snapshot_id=sid AND base_offset>=start_offset
  ORDER BY base_offset LIMIT 128 LOOP
  IF used+item.bytes>2097152 THEN EXIT; END IF;
  IF item.record_count<=0 OR item.base_offset<>next_offset OR item.base_offset+item.record_count>snap.end_offset THEN
   RAISE EXCEPTION 'Snapshot batch range is invalid' USING ERRCODE='55000';
  END IF;
  next_offset:=item.base_offset+item.record_count;
  used:=used+item.bytes;
 END LOOP;
 IF next_offset=start_offset AND start_offset<snap.end_offset THEN
  RAISE EXCEPTION 'Snapshot page missing or exceeds byte budget' USING ERRCODE='55000';
 END IF;
 SELECT coalesce(jsonb_agg(jsonb_build_object('base_offset',base_offset,'record_count',record_count,
   'data',encode(data,'hex'),'max_timestamp',max_timestamp,'timestamps',timestamps,
   'appended_at_micros',(extract(epoch FROM appended_at)*1000000)::bigint) ORDER BY base_offset),'[]')
 INTO batches FROM broker.raft_snapshot_batches
 WHERE snapshot_id=sid AND base_offset>=start_offset AND base_offset<next_offset;
 result:=jsonb_build_object('snapshot',to_jsonb(snap),'batches',batches,'next_offset',next_offset,'eof',next_offset=snap.end_offset);
 IF octet_length(result::text)>8388608 THEN RAISE EXCEPTION 'Snapshot response byte limit' USING ERRCODE='54000'; END IF;
 RETURN result;
END $$;
REVOKE ALL ON FUNCTION public.broker_raft_snapshot_read(text,bigint,uuid,bigint) FROM PUBLIC;
