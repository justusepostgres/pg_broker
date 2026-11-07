-- Experimental Raft vote persistence only. Default installation stays 0.1.7.
-- A writer epoch fences competing local storage owners; it is NOT a Raft term
-- or evidence of quorum leadership. The typed Rust adapter orders votes.
CREATE TABLE broker.raft_votes (
 group_key text PRIMARY KEY CHECK (octet_length(group_key) BETWEEN 1 AND 249),
 writer_epoch bigint NOT NULL CHECK (writer_epoch > 0),
 revision bigint NOT NULL DEFAULT 0 CHECK (revision >= 0),
 vote jsonb CHECK (vote IS NULL OR (jsonb_typeof(vote)='object' AND octet_length(vote::text)<=1024))
);
SELECT pg_catalog.pg_extension_config_dump('broker.raft_votes','');

CREATE FUNCTION public.broker_raft_vote_claim(k text) RETURNS jsonb
LANGUAGE plpgsql VOLATILE STRICT SECURITY DEFINER SET search_path=pg_catalog,broker AS $$
DECLARE r broker.raft_votes;
BEGIN
 INSERT INTO broker.raft_votes(group_key,writer_epoch) VALUES(k,1)
 ON CONFLICT(group_key) DO UPDATE SET writer_epoch=broker.raft_votes.writer_epoch+1
 RETURNING * INTO r;
 RETURN jsonb_build_object('epoch',r.writer_epoch,'revision',r.revision,'vote',r.vote);
END $$;

CREATE FUNCTION public.broker_raft_vote_read(k text,e bigint) RETURNS jsonb
LANGUAGE plpgsql VOLATILE STRICT SECURITY DEFINER SET search_path=pg_catalog,broker AS $$
DECLARE r broker.raft_votes;
BEGIN
 SELECT * INTO r FROM broker.raft_votes WHERE group_key=k AND writer_epoch=e FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'Raft vote writer fenced' USING ERRCODE='55000'; END IF;
 RETURN jsonb_build_object('epoch',r.writer_epoch,'revision',r.revision,'vote',r.vote);
END $$;

CREATE FUNCTION public.broker_raft_vote_cas(k text,e bigint,rev bigint,v jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE STRICT SECURITY DEFINER SET search_path=pg_catalog,broker AS $$
DECLARE r broker.raft_votes;
BEGIN
 UPDATE broker.raft_votes SET vote=v,revision=revision+1
 WHERE group_key=k AND writer_epoch=e AND revision=rev RETURNING * INTO r;
 IF NOT FOUND THEN RAISE EXCEPTION 'Raft vote writer fenced or revision changed' USING ERRCODE='55000'; END IF;
 RETURN jsonb_build_object('epoch',r.writer_epoch,'revision',r.revision,'vote',r.vote);
END $$;
REVOKE ALL ON FUNCTION public.broker_raft_vote_claim(text) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.broker_raft_vote_read(text,bigint) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.broker_raft_vote_cas(text,bigint,bigint,jsonb) FROM PUBLIC;
