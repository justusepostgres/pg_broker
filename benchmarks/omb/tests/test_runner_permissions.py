"""SQL copied into a container must remain readable by PostgreSQL's UID."""
from pathlib import Path
import os
import subprocess
import tempfile
import unittest


class MigrationPermissionTests(unittest.TestCase):
    def test_private_upload_becomes_readable_migration(self):
        runner=(Path(__file__).resolve().parents[1]/'oci-pair.sh').read_text()
        start=runner.index('for migration in "${migrations[@]}"; do')
        block=runner[start:runner.index('\ndone',start)+len('\ndone')]
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary)
            sql=root/'crates/extension/sql';sql.mkdir(parents=True)
            out=root/'out';out.mkdir()
            name='pg_broker--0.1.16--0.1.17.sql'
            source=sql/name;source.write_text('SELECT 1;\n');source.chmod(0o600)
            subprocess.run(['bash','-c','umask 077\nmigrations=(0.1.16--0.1.17)\n'+block],
                           env=dict(os.environ,repo=str(root),out=str(out)),check=True)
            self.assertEqual((out/name).read_text(),source.read_text())
            self.assertEqual((out/name).stat().st_mode & 0o777,0o644)
            self.assertEqual(source.stat().st_mode & 0o777,0o600)
