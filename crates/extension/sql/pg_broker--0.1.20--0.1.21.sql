-- Experimental storage generations for future atomic snapshot activation.
-- No snapshot installer is exposed by this migration. Default remains 0.1.7.
ALTER TABLE broker.partitions ADD COLUMN storage_generation uuid NOT NULL
 DEFAULT '00000000-0000-0000-0000-000000000000';
ALTER TABLE broker.batches RENAME TO batch_storage;
ALTER TABLE broker.batch_storage ADD COLUMN storage_generation uuid NOT NULL
 DEFAULT '00000000-0000-0000-0000-000000000000';
-- Existing rows retain the zero generation. New inserts use NULL as an
-- omitted-generation sentinel, so COPY/restore of an explicit zero is preserved.
ALTER TABLE broker.batch_storage ALTER COLUMN storage_generation DROP DEFAULT;
ALTER TABLE broker.batch_storage DROP CONSTRAINT batches_pkey;
ALTER TABLE broker.batch_storage ADD PRIMARY KEY(topic,partition,storage_generation,base_offset);

-- Single base relation: PostgreSQL's supported automatically updatable view.
-- The config-dump registration follows the renamed base table's OID, preserving
-- all generations in logical backups. The view itself has no persisted data.
CREATE VIEW broker.batches AS
 SELECT topic,partition,base_offset,record_count,data,max_timestamp,timestamps,appended_at
 FROM broker.batch_storage b
 WHERE storage_generation=(SELECT p.storage_generation FROM broker.partitions p
                           WHERE p.topic=b.topic AND p.partition=b.partition)
 WITH CASCADED CHECK OPTION;

CREATE OR REPLACE FUNCTION broker.raft_guard_batches() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog,broker AS $$
BEGIN
 IF TG_OP<>'INSERT' THEN PERFORM broker.raft_require_application(OLD.topic,OLD.partition); END IF;
 IF TG_OP<>'DELETE' THEN
  PERFORM broker.raft_require_application(NEW.topic,NEW.partition);
  -- Legacy inserts omit the generation. Select it only after acquiring the
  -- same partition lock used by allocation/application/activation.
  IF TG_OP='INSERT' AND NEW.storage_generation IS NULL THEN
   SELECT storage_generation INTO NEW.storage_generation FROM broker.partitions
    WHERE topic=NEW.topic AND partition=NEW.partition;
  END IF;
  RETURN NEW;
 END IF;
 RETURN OLD;
END $$;

CREATE OR REPLACE FUNCTION broker.raft_snapshot_preserve_source() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog,broker AS $$
BEGIN
 PERFORM 1 FROM broker.partitions WHERE topic=OLD.topic AND partition=OLD.partition FOR UPDATE;
 IF OLD.storage_generation=(SELECT storage_generation FROM broker.partitions
                            WHERE topic=OLD.topic AND partition=OLD.partition)
    AND EXISTS(SELECT 1 FROM broker.raft_snapshots WHERE topic=OLD.topic AND partition=OLD.partition
               AND state='capturing' AND OLD.base_offset<end_offset
               AND OLD.base_offset+OLD.record_count>earliest) THEN
  RAISE EXCEPTION 'Snapshot source range is pinned' USING ERRCODE='55000';
 END IF;
 IF TG_OP='DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF;
END $$;

-- Invalidate cached PL/pgSQL bodies that resolved the former table name/OID.
-- Recreating the same signatures preserves function OIDs and explicit grants.
DO $$
DECLARE definition text;
BEGIN
 FOR definition IN SELECT pg_get_functiondef(p.oid) FROM pg_proc p
   JOIN pg_namespace n ON n.oid=p.pronamespace JOIN pg_language l ON l.oid=p.prolang
   WHERE n.nspname IN ('broker','public') AND l.lanname IN ('sql','plpgsql')
     AND p.prosrc LIKE '%broker.batches%'
 LOOP EXECUTE definition; END LOOP;
END $$;

-- Private table access must not expose inactive snapshot generations. Reset
-- non-owner grants on both internal relations, including inherited defaults.
DO $$
DECLARE item record;
BEGIN
 FOR item IN
  SELECT DISTINCT c.oid::regclass relation,r.rolname
   FROM pg_class c,
    LATERAL aclexplode(coalesce(c.relacl,acldefault('r',c.relowner))) a
    JOIN pg_roles r ON r.oid=a.grantee
   WHERE c.oid IN ('broker.batch_storage'::regclass,'broker.batches'::regclass)
    AND a.grantee<>c.relowner
 LOOP EXECUTE format('REVOKE ALL ON TABLE %s FROM %I',item.relation,item.rolname); END LOOP;
END $$;
REVOKE ALL ON broker.batch_storage,broker.batches FROM PUBLIC;
