"""Bounded retries for observation and downloads, never workload commands."""
import re
import subprocess
import time
from pathlib import Path
from remote_client import ssh


def _retry(operation, attempts=3):
    if not 1 <= attempts <= 5:
        raise ValueError('attempt limit must be 1..5')
    for attempt in range(attempts):
        try:
            return operation()
        except (subprocess.TimeoutExpired, subprocess.CalledProcessError) as error:
            if isinstance(error, subprocess.CalledProcessError) and error.returncode != 255:
                raise
            if attempt+1==attempts:
                raise
            time.sleep(min(2**attempt,8))


def service(state, unit, invocation):
    if not re.fullmatch(r'omb-[a-z0-9-]+\.service',unit) or not re.fullmatch(r'[a-f0-9]{32}',invocation):
        raise ValueError('invalid service identity')
    result=_retry(lambda:ssh(state,['systemctl','show',unit,'-p','InvocationID','-p','MainPID',
                                   '-p','SubState','-p','Result','-p','ExecMainStatus'],
                            capture_output=True,text=True,check=True))
    observed=dict(line.split('=',1) for line in result.stdout.splitlines())
    if observed.get('InvocationID')!=invocation:
        raise ValueError('service invocation changed; never follow a replacement')
    if not observed.get('MainPID','').isdigit():
        raise ValueError('missing service process state')
    return observed


def download_completed(state, unit, invocation, remote, destination):
    """Download only a successful exact invocation; preserve failed partial files."""
    if not re.fullmatch(r'/home/ubuntu/results/omb-pair-[a-z0-9-]+',remote):
        raise ValueError('invalid result directory')
    def verify():
        observed=service(state,unit,invocation)
        if not (observed['MainPID']=='0' and observed.get('SubState')=='exited'
                and observed.get('Result')=='success' and observed.get('ExecMainStatus')=='0'):
            raise ValueError('original service has not completed successfully')
    verify()
    _retry(lambda:ssh(state,['test','-s',remote+'/complete.txt'],capture_output=True,check=True))
    destination=Path(destination)
    # Exclusive creation protects earlier evidence. Each transport retry starts
    # this new partial archive at byte zero; never concatenate compressed streams.
    with destination.open('xb') as output:
        def transfer():
            output.seek(0);output.truncate()
            ssh(state,['tar','-czf','-','-C',remote,'.'],stdout=output,check=True)
            output.flush()
        _retry(transfer)
    verify()
    if destination.stat().st_size>256*1024*1024:
        raise ValueError('archive exceeds collection limit')
    return destination
