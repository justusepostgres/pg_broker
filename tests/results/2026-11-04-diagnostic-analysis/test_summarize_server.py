import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

spec=importlib.util.spec_from_file_location('summarize_server',Path(__file__).resolve().parents[1]/'summarize-server.py')
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)


class SummaryTests(unittest.TestCase):
    def summarize(self, rows):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'samples.jsonl'
            path.write_text(''.join(json.dumps(r)+'\n' for r in rows))
            return module.summarize(path,'2026-11-04T00:00:00Z','2026-11-04T00:00:06Z')

    def row(self, second, usage):
        return dict(at=f'2026-11-04T00:00:{second:02d}Z',waits=[dict(
            backend_type='client backend',state='active',wait_event_type='IO',wait_event='WalSync',backends=1)],
            cgroups={'db':{'cpu.stat':f'usage_usec {usage}\nuser_usec {usage}\nsystem_usec 0\nthrottled_usec 0\n',
                            'memory.current':'100'}})

    def test_cpu_units_and_wait_observation_counts(self):
        result=self.summarize([self.row(0,100),self.row(2,2000100),self.row(4,4000100)])
        self.assertEqual(result['mean_cpu_cores'],{'db:usage_usec':1.0})
        self.assertEqual(result['covered_seconds'],4)
        self.assertEqual(result['active_backend_observations'],{'IO:WalSync':3})

    def test_errors_are_gaps_not_zero_cpu_or_interpolated_samples(self):
        result=self.summarize([self.row(0,100),dict(at='2026-11-04T00:00:02Z',error='timeout'),self.row(4,4000100)])
        self.assertEqual(result['sample_errors'],1)
        self.assertEqual(result['covered_seconds'],0)
        self.assertEqual(result['mean_cpu_cores'],{})

    def test_counter_reset_and_reversed_clock_rejected(self):
        for rows in ([self.row(0,100),self.row(2,0)], [self.row(2,100),self.row(0,200)]):
            with self.assertRaises(ValueError):self.summarize(rows)

    def snapshots(self, timing='off'):
        def snapshot(second, value):
            return dict(captured_at=f'2026-11-04T00:00:{second:02d}Z',wal_io_timing=timing,
                wal=dict(stats_reset='2026-11-03T00:00:00Z', **{key:value for key in (
                    'wal_records','wal_fpi','wal_bytes','wal_buffers_full','wal_write','wal_sync',
                    'wal_write_time','wal_sync_time')}),
                database=dict(stats_reset='2026-11-03T00:00:00Z', **{key:value for key in (
                    'xact_commit','xact_rollback','blks_read','blks_hit','tup_inserted','tup_updated','tup_deleted')}))
        return snapshot(0,10),snapshot(10,20)

    def test_disabled_wal_timing_is_missing_not_zero_latency(self):
        result=module.database_delta(*self.snapshots())
        self.assertIsNone(result['wal_sync_mean_ms'])
        self.assertIsNone(result['wal_write_mean_ms'])
        self.assertEqual(result['wal_bytes_per_second'],1)

    def test_enabled_wal_timing_uses_counter_deltas(self):
        before,after=self.snapshots('on')
        after['wal']['wal_sync_time']=30
        self.assertEqual(module.database_delta(before,after)['wal_sync_mean_ms'],2)
        after['wal_io_timing']='off'
        self.assertIsNone(module.database_delta(before,after)['wal_sync_mean_ms'])

    def test_database_statistics_reset_rejected(self):
        before,after=self.snapshots()
        after['wal']['stats_reset']='2026-11-04T00:00:01Z'
        with self.assertRaisesRegex(ValueError,'statistics reset'):
            module.database_delta(before,after)
