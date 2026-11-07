import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
import test_compare
import read_only_collection as module


class ReadOnlyCollectionTests(unittest.TestCase):
    def test_service_transport_retry_preserves_identity(self):
        good=subprocess.CompletedProcess([],0,stdout='InvocationID='+'a'*32+'\nMainPID=42\n')
        with patch.object(module,'ssh',side_effect=[subprocess.CalledProcessError(255,'ssh'),good]) as ssh,patch.object(module.time,'sleep'):
            self.assertEqual(module.service({},'omb-fixture.service','a'*32)['MainPID'],'42')
            self.assertEqual(ssh.call_args_list[0],ssh.call_args_list[1])
        with patch.object(module,'ssh',return_value=good):
            with self.assertRaisesRegex(ValueError,'invocation changed'):module.service({},'omb-fixture.service','b'*32)

    def test_nontransport_failure_is_not_retried(self):
        with patch.object(module,'ssh',side_effect=subprocess.CalledProcessError(1,'test')) as ssh:
            with self.assertRaises(subprocess.CalledProcessError):module.service({},'omb-fixture.service','a'*32)
            self.assertEqual(ssh.call_count,1)

    def test_download_retry_truncates_partial_stream_and_checks_same_service(self):
        done=dict(MainPID='0',SubState='exited',Result='success',ExecMainStatus='0')
        writes=0
        def ssh(state,args,**kwargs):
            nonlocal writes
            if args[0]=='tar':
                writes+=1
                kwargs['stdout'].write(b'partial' if writes==1 else b'complete')
                if writes==1:raise subprocess.CalledProcessError(255,'ssh')
            return subprocess.CompletedProcess(args,0)
        with tempfile.TemporaryDirectory() as temporary,patch.object(module,'service',return_value=done) as status,patch.object(module,'ssh',side_effect=ssh),patch.object(module.time,'sleep'):
            target=Path(temporary)/'archive.partial'
            args=({},'omb-fixture.service','a'*32,'/home/ubuntu/results/omb-pair-fixture',target)
            module.download_completed(*args)
            self.assertEqual(target.read_bytes(),b'complete')
            self.assertEqual(status.call_count,2)
            with self.assertRaises(FileExistsError):module.download_completed(*args)

    def test_running_or_failed_run_cannot_be_downloaded(self):
        for state in (dict(MainPID='5'),dict(MainPID='0',SubState='failed')):
            with patch.object(module,'service',return_value=state),patch.object(module,'ssh') as ssh:
                with self.assertRaisesRegex(ValueError,'not completed'):module.download_completed({},'omb-fixture.service','a'*32,'/home/ubuntu/results/omb-pair-fixture','unused')
                ssh.assert_not_called()
