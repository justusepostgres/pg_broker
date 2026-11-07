import hashlib
import shutil
import unittest
from unittest.mock import patch
import test_compare_partition_guard as fixtures
import compare_primary_pipeline as module


class PrimaryPipelineComparisonTests(unittest.TestCase):
    def setUp(self):
        fixture=fixtures.PartitionGuardComparisonTests();fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        self.old=fixture.new;self.new=self.old.parent/'pipeline'
        shutil.copytree(self.old,self.new)
        digest=hashlib.sha256(b'fixture').hexdigest()
        guard=patch.object(module,'SOURCES',{name:digest for name in module.SOURCES})
        guard.start();self.addCleanup(guard.stop)
        self.sha='b'*64
        (self.new/'gateway-sha256.txt').write_text(self.sha+'  /new')
        p=self.new/'broker-limits.txt';p.write_text(p.read_text().replace('/guard-','/pipeline-'))

    def test_compares_only_expected_binary_change(self):
        result=module.compare(self.old,self.new,self.sha)
        self.assertEqual(result['candidate_publish_rate_ratio'],1)
        self.assertEqual(result['candidate_sha256'],self.sha)

    def test_rejects_unexpected_binary_or_sql(self):
        with self.assertRaisesRegex(ValueError,'unexpected gateway'):module.compare(self.old,self.new,'c'*64)
        (self.new/'pg_broker--0.1.17--0.1.18.sql').write_text('changed')
        with self.assertRaisesRegex(ValueError,'unreviewed'):module.compare(self.old,self.new,self.sha)

    def test_rejects_changed_resources(self):
        (self.new/'volume-profile.json').write_text('changed')
        with self.assertRaisesRegex(ValueError,'configuration differs'):module.compare(self.old,self.new,self.sha)
