import hashlib
import json
import shutil
import unittest
from unittest.mock import patch
import test_compare_bulk_commit as fixtures
import compare_partition_guard as module


class PartitionGuardComparisonTests(unittest.TestCase):
    def setUp(self):
        fixture=fixtures.BulkComparisonTests();fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        self.old=fixture.new;self.new=self.old.parent/'guard'
        shutil.copytree(self.old,self.new)
        digest=hashlib.sha256(b'fixture').hexdigest()
        for field in ('BASELINE_SOURCES','SOURCES'):
            guard=patch.object(module,field,{name:digest for name in getattr(module,field)})
            guard.start();self.addCleanup(guard.stop)
        for name in module.SOURCES:(self.new/name).write_text('fixture')
        (self.new/'storage.txt').write_text('0.1.18|on|on|on|t|t\n16|t\n')
        p=self.new/'instrumentation.json';data=json.loads(p.read_text())
        data['sql_version']='0.1.18';p.write_text(json.dumps(data))
        p=self.new/'broker-limits.txt';p.write_text(p.read_text().replace('/candidate-','/guard-'))
        for directory,count in ((self.old,20),(self.new,10)):
            p=directory/'diagnostic-stats-after.json';data=json.loads(p.read_text())
            data['functions'] += [dict(funcid=124,schemaname='public',funcname='broker_append_bytes',calls=10,total_time=10,self_time=5),
                                  dict(funcid=125,schemaname='broker',funcname='raft_require_application',calls=count,total_time=4,self_time=4)]
            p.write_text(json.dumps(data))

    def test_same_binary_and_whole_lifetime_counts(self):
        report=module.compare(self.old,self.new)
        self.assertEqual(report['reference']['ownership_helper_calls_per_append_entry'],2)
        self.assertEqual(report['candidate']['ownership_helper_calls_per_append_entry'],1)
        self.assertIn('No repeatability',report['scope'])

    def test_rejects_missing_ownership_checks(self):
        p=self.new/'diagnostic-stats-after.json';data=json.loads(p.read_text())
        data['functions']=[f for f in data['functions'] if f['funcname']!='raft_require_application']
        p.write_text(json.dumps(data))
        with self.assertRaisesRegex(ValueError,'ownership checks'):module.compare(self.old,self.new)

    def test_rejects_binary_and_migration_changes(self):
        p=self.new/'gateway-sha256.txt';original=p.read_text();p.write_text('different')
        with self.assertRaisesRegex(ValueError,'configuration differs'):module.compare(self.old,self.new)
        p.write_text(original)
        (self.new/'pg_broker--0.1.17--0.1.18.sql').write_text('different')
        with self.assertRaisesRegex(ValueError,'unreviewed'):module.compare(self.old,self.new)
