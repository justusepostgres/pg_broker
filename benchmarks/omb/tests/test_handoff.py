import argparse
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import test_compare  # Adds the benchmark tools directory to sys.path.
import handoff_client


class HandoffTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.args = argparse.Namespace(server_host='192.0.2.1', server_private='10.1.0.1',
            client_host='192.0.2.2', key=self.root/'key', server_known_hosts=self.root/'server-hosts',
            client_known_hosts=self.root/'client-hosts', pair='fixture', system='broker')
        self.request = dict(label='fixture-broker', rate='32k', bootstrap='10.1.0.1:9092', system='broker')
        client = self.root/'client'
        (client/'artifacts').mkdir(parents=True)
        (client/'artifacts'/'complete.txt').write_text('fixture-complete\n')
        (client/'service-status.txt').write_text('fixture-status\n')
        (client/'control.json').write_text(json.dumps(dict(host='192.0.2.2', label='fixture-broker',
            rate='32k', bootstrap='10.1.0.1:9092', preflight=False, phase='complete', invocation='fixture-id')))

    def test_lost_delivery_response_reuses_exact_archive_without_relaunch(self):
        transfers = []
        finalizations = []
        def remote(state, command, **kwargs):
            if command[0] == 'cat':
                return subprocess.CompletedProcess(command, 0, stdout=json.dumps(self.request))
            if command[-1].startswith('cat > '):
                transfers.append(kwargs['stdin'].read())
                return subprocess.CompletedProcess(command, 0)
            finalizations.append(command[-1])
            if len(finalizations) == 1:
                # The remote rename succeeded, but its response was lost.
                raise subprocess.CalledProcessError(255, command)
            return subprocess.CompletedProcess(command, 0)
        with patch.object(handoff_client, 'ssh', side_effect=remote), patch.object(
                handoff_client.subprocess, 'run') as collect:
            with self.assertRaises(subprocess.CalledProcessError):
                handoff_client.advance(self.args, self.root)
            self.assertFalse((self.root/'delivered.json').exists())
            (self.root/'client/artifacts/complete.txt').write_text('changed after first transfer\n')
            handoff_client.advance(self.args, self.root)
            self.assertEqual(len(transfers), 2)
            self.assertEqual(transfers[0], transfers[1])
            self.assertEqual(finalizations[0], finalizations[1])
            self.assertTrue((self.root/'delivered.json').exists())
            for call in collect.call_args_list:
                self.assertEqual(call.args[0][2], 'collect')
            handoff_client.advance(self.args, self.root)
            self.assertEqual(len(transfers), 2, 'already delivered data must not transfer again')
            self.assertEqual(collect.call_count, 2, 'delivered phases do not depend on surviving remote units')

    def test_changed_request_is_rejected_before_touching_client(self):
        self.request['bootstrap'] = '10.1.0.9:9092'
        with patch.object(handoff_client, 'ssh', return_value=subprocess.CompletedProcess(
                [], 0, stdout=json.dumps(self.request))), patch.object(handoff_client.subprocess, 'run') as run:
            with self.assertRaisesRegex(ValueError, 'does not match'):
                handoff_client.advance(self.args, self.root)
            run.assert_not_called()

    def test_wait_reuses_same_control_directory_until_delivery(self):
        with patch.object(handoff_client, 'advance', side_effect=[False, False, True]) as advance, \
                patch.object(handoff_client.time, 'sleep') as sleep:
            handoff_client.wait_for_delivery(self.args, self.root)
        self.assertEqual(advance.call_count, 3)
        for call in advance.call_args_list:
            self.assertEqual(call.args, (self.args, self.root))
        self.assertEqual([call.args for call in sleep.call_args_list], [(30,), (30,)])

    def test_wait_timeout_does_not_delete_or_replace_durable_handle(self):
        control = self.root/'client/control.json'
        before = control.read_bytes()
        with patch.object(handoff_client, 'advance', return_value=False) as advance, \
                patch.object(handoff_client.time, 'monotonic', side_effect=[0, 1801]), \
                patch.object(handoff_client.time, 'sleep') as sleep:
            with self.assertRaisesRegex(TimeoutError, 'remote run was not stopped'):
                handoff_client.wait_for_delivery(self.args, self.root)
        self.assertEqual(advance.call_count, 1)
        sleep.assert_not_called()
        self.assertEqual(control.read_bytes(), before)

    def test_wait_observation_failure_is_not_retried_as_a_new_launch(self):
        with patch.object(handoff_client, 'advance', side_effect=subprocess.CalledProcessError(255, 'ssh')) as advance, \
                patch.object(handoff_client.time, 'sleep') as sleep:
            with self.assertRaises(subprocess.CalledProcessError):
                handoff_client.wait_for_delivery(self.args, self.root)
        self.assertEqual(advance.call_count, 1)
        sleep.assert_not_called()


if __name__ == '__main__':
    unittest.main()
