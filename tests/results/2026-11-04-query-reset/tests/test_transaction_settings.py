"""Pipelined queries must observe settings before their first durable write."""
import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
from kafka import KafkaProducer

from conftest import DSN, Gateway
from test_broker import create, name


def test_settings_precede_cold_and_cached_append_queries(gateway):
    topic, function, trigger = create(), name(), name()
    actor = Gateway(make_conninfo(DSN, options='-c synchronous_commit=off -c statement_timeout=9s -c lock_timeout=8s'), 19099)
    with psycopg.connect(DSN, autocommit=True) as admin:
        admin.execute(sql.SQL('''CREATE FUNCTION broker.{}() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          IF current_setting('synchronous_commit') <> 'on'
             OR current_setting('statement_timeout') <> '3s'
             OR current_setting('lock_timeout') <> '2s' THEN
            RAISE EXCEPTION 'append ran before transaction settings';
          END IF;
          RETURN NEW;
        END $$''').format(sql.Identifier(function)))
        try:
            admin.execute(sql.SQL('''CREATE TRIGGER {} BEFORE INSERT ON broker.batches
              FOR EACH ROW WHEN (NEW.topic = {}) EXECUTE FUNCTION broker.{}()''').format(
                sql.Identifier(trigger), sql.Literal(topic), sql.Identifier(function)))
            try:
                actor.start()
                p = KafkaProducer(bootstrap_servers='127.0.0.1:19099', acks='all', retries=0,
                                  request_timeout_ms=10000, max_block_ms=10000)
                try:
                    for offset in range(6):
                        assert p.send(topic, partition=0, value=b'ordered-settings').get(10).offset == offset
                finally:
                    p.close(timeout=5)
            finally:
                actor.stop()
                actor.log.close()
                admin.execute(sql.SQL('DROP TRIGGER {} ON broker.batches').format(sql.Identifier(trigger)))
        finally:
            admin.execute(sql.SQL('DROP FUNCTION broker.{}()').format(sql.Identifier(function)))
