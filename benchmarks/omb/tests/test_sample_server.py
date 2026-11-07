import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location('sample_server', Path(__file__).resolve().parents[1] / 'sample-server.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class SamplerTests(unittest.TestCase):
    def test_unified_container_cgroup(self):
        self.assertEqual(module.cgroup_path('0::/system.slice/docker-abc.scope\n'),
                         Path('/sys/fs/cgroup/system.slice/docker-abc.scope'))

    def test_invalid_or_ambiguous_paths_rejected(self):
        for value in ('0::/../../etc', '1:cpu:/docker/a', '0::relative', '0::/one\n0::/two'):
            with self.subTest(value=value), self.assertRaises(ValueError):
                module.cgroup_path(value)
