import importlib.util,json,subprocess
from pathlib import Path
p=Path('/home/ubuntu/results/omb-pair-017-timing100k-1')
spec=importlib.util.spec_from_file_location('sampler',p/'sample-server.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
after=json.loads(subprocess.check_output(['sudo','docker','exec','-e','PGOPTIONS=-c statement_timeout=1000','omb-pair-017-timing100k-1-db','psql','-X','-U','postgres','-At','-v','ON_ERROR_STOP=1','-c',m.STATS],text=True,timeout=8))
print(json.dumps(dict(before=json.loads((p/'diagnostic-stats-before.json').read_text()),after=after,samples=(p/'samples.jsonl').read_text())))
