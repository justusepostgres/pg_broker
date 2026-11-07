import unittest
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from client_events import client_events


class ClientEventsTests(unittest.TestCase):
    def test_info_fetch_diagnostic_is_visible_without_inventing_errors(self):
        log = '''02:38:54.714 [main] INFO FetchSessionHandler - [Consumer clientId=x] Node 1 sent an invalid full fetch response with extraPartitions=(t-0)
02:38:54.715 [main] INFO Other - sent an invalid full fetch response
02:38:54.716 [main] WARN org.apache.kafka.clients.FetchSessionHandler - Node 1 sent an invalid incremental fetch response with missingPartitions=(t-0)
'''
        result = client_events(log)
        self.assertEqual(result['invalid_fetch_response_events'], 2)
        self.assertEqual(result['warning_events'], 1)
        self.assertEqual(result['error_events'], 0)

    def test_counts_events_not_exception_stack_frames(self):
        log = '''23:16:52.440 [pool-3-thread-1] ERROR ConsumerCoordinator - [Consumer clientId=x] Offset commit with offsets {a=1} failed
org.apache.kafka.clients.consumer.RetriableCommitFailedException: Offset commit failed
Caused by: org.apache.kafka.common.errors.TimeoutException: expired
23:16:52.441 [thread] WARN NetworkClient - request timed out
23:16:52.442 [thread] ERROR Other - unrelated failure
23:16:52.443 [thread] INFO Other - ERROR is merely message text
'''
        result = client_events(log)
        self.assertEqual(result['offset_commit_failure_events'], 1)
        self.assertEqual(result['error_events'], 2)
        self.assertEqual(result['warning_events'], 1)
        self.assertIn('not lost-record counts', result['scope'])

    def test_empty_and_different_format_do_not_invent_events(self):
        for log in ('', 'ERROR ConsumerCoordinator - Offset commit with offsets {} failed'):
            result = client_events(log)
            self.assertEqual(result['error_events'], 0)
            self.assertEqual(result['offset_commit_failure_events'], 0)
