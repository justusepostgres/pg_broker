import hashlib
import json
import unittest
from unittest.mock import patch
import test_compare_commit_delay as fixtures
import compare_bulk_commit as module


class BulkComparisonTests(unittest.TestCase):
    def setUp(self):
        fixture=fixtures.CommitDelayTests()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        self.old,self.new=fixture.old,fixture.new
        digest=hashlib.sha256(b'fixture').hexdigest()
        guard=patch.object(module,'SOURCES',{name:digest for name in module.SOURCES})
        guard.start();self.addCleanup(guard.stop)
        for name in module.SOURCES:(self.new/name).write_text('fixture')
        (self.old/'gateway-sha256.txt').write_text(module.BASELINE_BINARY+'  /old')
        (self.new/'gateway-sha256.txt').write_text(module.CANDIDATE_BINARY+'  /new')
        (self.new/'storage.txt').write_text('0.1.17|on|on|on|t|t\n16|t\n')
        p=self.new/'instrumentation.json';data=json.loads(p.read_text())
        data.update(sql_version='0.1.17',gateway_changed=True);p.write_text(json.dumps(data))
        for name in ('before','after'):
            p=self.new/f'diagnostic-stats-{name}.json';data=json.loads(p.read_text())
            data.update(commit_delay='0',commit_siblings='5')
            if name=='after':
                data['functions']=[dict(funcid=123,schemaname='broker',funcname='commit_many',calls=10,total_time=2,self_time=1)]
            p.write_text(json.dumps(data))

    def test_reports_whole_candidate_and_preserves_overload(self):
        report=module.compare(self.old,self.new)
        self.assertEqual(report['candidate_publish_rate_ratio'],1)
        self.assertEqual(report['candidate']['publish_delay_p99_us'],60000255)
        self.assertIn('Not an isolated causal estimate',report['scope'])

    def test_rejects_unexercised_bulk_path(self):
        p=self.new/'diagnostic-stats-after.json';data=json.loads(p.read_text())
        data['functions']=[];p.write_text(json.dumps(data))
        with self.assertRaisesRegex(ValueError,'bulk path'):module.compare(self.old,self.new)

    def test_rejects_migration_and_binary_substitution(self):
        p=self.new/'pg_broker--0.1.16--0.1.17.sql';p.write_text('different')
        with self.assertRaisesRegex(ValueError,'unreviewed'):module.compare(self.old,self.new)
        p.write_text('fixture')
        (self.new/'gateway-sha256.txt').write_text('bad /path')
        with self.assertRaisesRegex(ValueError,'unexpected gateway'):module.compare(self.old,self.new)

    def test_rejects_changed_workload_and_commit_delay(self):
        p=self.new/'broker.json.workload.yaml';p.write_text('changed')
        with self.assertRaisesRegex(ValueError,'configuration differs'):module.compare(self.old,self.new)
        p.write_text('fixture')
        p=self.new/'diagnostic-stats-after.json';data=json.loads(p.read_text())
        data['commit_delay']='500';p.write_text(json.dumps(data))
        with self.assertRaisesRegex(ValueError,'settings differ'):module.compare(self.old,self.new)
