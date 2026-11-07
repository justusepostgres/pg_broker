-- Opt-in distributed-storage prerequisite. Default installation stays at 0.1.7
-- until routing and storage-enforced leader fencing are implemented and tested.
ALTER TABLE broker.topics ADD COLUMN topic_id uuid NOT NULL DEFAULT pg_catalog.gen_random_uuid();
ALTER TABLE broker.topics ADD CONSTRAINT topics_topic_id_key UNIQUE(topic_id);

CREATE FUNCTION broker.preserve_topic_identity() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog,broker AS $$
BEGIN
 RAISE EXCEPTION 'topic identity cannot be changed' USING ERRCODE='22023';
END $$;
REVOKE ALL ON FUNCTION broker.preserve_topic_identity() FROM PUBLIC;
CREATE TRIGGER preserve_topic_identity BEFORE UPDATE OF topic_id ON broker.topics
FOR EACH ROW WHEN (OLD.topic_id IS DISTINCT FROM NEW.topic_id)
EXECUTE FUNCTION broker.preserve_topic_identity();

-- Checking and appending must share one transaction. KEY SHARE prevents a
-- concurrent delete/recreate from replacing this topic between these operations,
-- while allowing appends to different partitions of the same topic concurrently.
-- This is an identity check, not a leader epoch/replication fence. Legacy entry
-- points remain available; gateways do not advertise distributed storage yet.
CREATE FUNCTION public.broker_append_identified(t text,p integer,tid uuid,bytes bytea)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path=pg_catalog,broker AS $$
BEGIN
 IF t IS NULL OR p IS NULL OR tid IS NULL OR bytes IS NULL THEN
  RETURN '{"error":42}';
 END IF;
 PERFORM 1 FROM broker.topics WHERE name=t AND topic_id=tid FOR KEY SHARE;
 IF NOT FOUND THEN RETURN '{"error":3}'; END IF;
 -- Reuse pgrx's full record-set validation and transactional offset allocator.
 RETURN public.broker_append_bytes(t,p,bytes);
END $$;
REVOKE ALL ON FUNCTION public.broker_append_identified(text,integer,uuid,bytea) FROM PUBLIC;
