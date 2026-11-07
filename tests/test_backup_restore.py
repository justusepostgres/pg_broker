"""A real PostgreSQL logical archive restored into a fresh database."""
import os
from pathlib import Path
import subprocess
import uuid
import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
from kafka import KafkaProducer, KafkaConsumer, TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.structs import OffsetAndMetadata
from conftest import DSN, Gateway


def test_logical_backup_restores_records_groups_and_bounds(tmp_path):
    pg_config = os.getenv('PG_CONFIG', 'pg_config')
    bindir = Path(subprocess.check_output([pg_config, '--bindir'], text=True).strip())
    names = ['backup_' + uuid.uuid4().hex, 'restore_' + uuid.uuid4().hex]
    dsns = [make_conninfo(DSN, dbname=name) for name in names]
    gateway = Gateway(dsns[0], 19094)
    address = '127.0.0.1:19094'
    created = []
    with psycopg.connect(DSN, autocommit=True) as admin:
        try:
            for name in names:
                admin.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(name)))
                created.append(name)
            with psycopg.connect(dsns[0], autocommit=True) as conn:
                conn.execute('CREATE EXTENSION pg_broker')
            gateway.start()
            a = KafkaAdminClient(bootstrap_servers=address)
            a.create_topics([NewTopic('archive', 2, 1)]); a.close()
            p = KafkaProducer(bootstrap_servers=address, acks='all')
            for partition in range(2):
                for offset in range(3):
                    assert p.send('archive', partition=partition, key=b'key',
                                  value=str(offset).encode(), timestamp_ms=1700000000000+offset,
                                  headers=[('backup', b'yes')]).get(10).offset == offset
            p.close()
            # Independent managed groups deliberately leave different checkpoints.
            for group, checkpoint in [('first', 1), ('second', 2)]:
                c = KafkaConsumer('archive', bootstrap_servers=address, group_id=group,
                                  enable_auto_commit=False, auto_offset_reset='earliest',
                                  consumer_timeout_ms=10000)
                records = []
                for record in c:
                    records.append(record)
                    if len(records) == 6: break
                assert len(records) == 6
                c.commit({TopicPartition('archive', n): OffsetAndMetadata(checkpoint, 'backup') for n in range(2)})
                c.close()
            gateway.stop()
            with psycopg.connect(dsns[0], autocommit=True) as conn:
                conn.execute("UPDATE broker.batches SET appended_at=now()-interval '10 days' WHERE base_offset=0")
                for partition in range(2):
                    conn.execute("SELECT broker_call(jsonb_build_object('op','retain','topic','archive','partition',%s,'retention_ms',86400000))", (partition,))
                before = {table: conn.execute(sql.SQL('SELECT * FROM broker.{} ORDER BY 1,2').format(sql.Identifier(table))).fetchall()
                          for table in ['topics', 'partitions', 'batches', 'groups', 'members', 'offsets']}
            archive = tmp_path / 'broker.dump'
            subprocess.run([str(bindir/'pg_dump'), '--format=custom', '--file', str(archive), '--dbname', dsns[0]], check=True, capture_output=True)
            subprocess.run([str(bindir/'pg_restore'), '--exit-on-error', '--single-transaction', '--dbname', dsns[1], str(archive)], check=True, capture_output=True)
            with psycopg.connect(dsns[1]) as conn:
                for table, rows in before.items():
                    assert conn.execute(sql.SQL('SELECT * FROM broker.{} ORDER BY 1,2').format(sql.Identifier(table))).fetchall() == rows
            gateway.dsn = dsns[1]; gateway.start()
            for group, checkpoint in [('first', 1), ('second', 2)]:
                c = KafkaConsumer(bootstrap_servers=address, group_id=group, enable_auto_commit=False,
                                  consumer_timeout_ms=5000)
                partitions = [TopicPartition('archive', n) for n in range(2)]
                c.assign(partitions)
                assert [c.committed(tp) for tp in partitions] == [checkpoint]*2
                assert set(c.beginning_offsets(partitions).values()) == {1}
                assert set(c.end_offsets(partitions).values()) == {3}
                restored = list(c)
                assert {(r.partition,r.offset) for r in restored} == {(p,o) for p in range(2) for o in range(checkpoint,3)}
                assert all(r.key==b'key' and r.headers==[('backup',b'yes')] and r.timestamp==1700000000000+r.offset for r in restored)
                c.close()
            p = KafkaProducer(bootstrap_servers=address, acks='all')
            assert p.send('archive',partition=0,value=b'after-restore').get(10).offset == 3
            p.close()
        finally:
            gateway.stop(); gateway.log.close()
            for name in reversed(created):
                admin.execute(sql.SQL('DROP DATABASE {} WITH (FORCE)').format(sql.Identifier(name)))
