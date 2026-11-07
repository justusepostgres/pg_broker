from pathlib import Path
import subprocess,os,shutil,json,time
root=Path.cwd();bindir=Path('/opt/homebrew/opt/postgresql@17/bin');data=root/'.dev/raft-guard-native-pg'
out=root/'tests/results/2026-11-04-raft-guard';out.mkdir(exist_ok=True)
assert not data.exists()
def run(*args):return subprocess.run([str(bindir/args[0]),*args[1:]],check=True,text=True,capture_output=True)
started=False
try:
 run('initdb','-D',str(data),'-U','postgres','--auth=trust')
 run('pg_ctl','-D',str(data),'-l',str(out/'postgres.log'),'-o','-p 55432 -h 127.0.0.1 -k /tmp','-w','start');started=True
 run('psql','-h','127.0.0.1','-p','55432','-U','postgres','-v','ON_ERROR_STOP=1','-c',"CREATE EXTENSION pg_broker VERSION '0.1.18'")
 env=dict(os.environ,DATABASE_URL='host=127.0.0.1 port=55432 user=postgres dbname=postgres',PG_BROKER_TEST_PGDATA=str(data),PG_BROKER_TEST_PG_CTL=str(bindir/'pg_ctl'),PG_BROKER_TEST_ALLOW_CRASH='1')
 results=[]
 for name,args in [('python',['.venv/bin/python','-m','pytest','-q','tests/test_raft_ownership.py'])]:
  start=time.monotonic()
  with (out/(name+'.log')).open('w') as f:r=subprocess.run(args,env=env,stdout=f,stderr=subprocess.STDOUT,timeout=180)
  print((out/(name+'.log')).read_text(),flush=True)
  results.append(dict(suite=name,exit_code=r.returncode,seconds=time.monotonic()-start))
  (out/'result.json').write_text(json.dumps(results,indent=2)+'\n')
  assert r.returncode==0,name
finally:
 if started:run('pg_ctl','-D',str(data),'-m','fast','-w','stop')
 if data.exists():shutil.rmtree(data)
