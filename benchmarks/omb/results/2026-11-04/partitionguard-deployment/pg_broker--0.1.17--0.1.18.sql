-- UPDATE already holds the OLD partition's row lock before this row trigger.
-- Avoid reacquiring it through a nested SPI query on every offset update.
-- Batch writes still use raft_require_application, including its explicit lock.
CREATE OR REPLACE FUNCTION broker.raft_guard_partition() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog,broker AS $$
BEGIN
 IF TG_OP='DELETE' THEN
  IF EXISTS(SELECT 1 FROM broker.raft_partition_state WHERE topic=OLD.topic AND partition=OLD.partition) THEN
   RAISE EXCEPTION 'Cannot delete a bound partition' USING ERRCODE='55000';
  END IF;
  RETURN OLD;
 END IF;
 IF NEW IS DISTINCT FROM OLD THEN
  IF EXISTS(SELECT 1 FROM broker.raft_partition_state s
            WHERE s.topic=OLD.topic AND s.partition=OLD.partition
              AND NOT EXISTS(SELECT 1 FROM broker.raft_apply_context c
                             WHERE c.group_key=s.group_key AND c.transaction_id=pg_current_xact_id())) THEN
   RAISE EXCEPTION 'Bound partition requires committed Raft application' USING ERRCODE='55000';
  END IF;
  IF (NEW.topic,NEW.partition) IS DISTINCT FROM (OLD.topic,OLD.partition) THEN
   PERFORM broker.raft_require_application(NEW.topic,NEW.partition);
  END IF;
 END IF;
 RETURN NEW;
END $$;
