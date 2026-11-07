from pathlib import Path
import subprocess,os,shutil,json
root=Path.cwd(); bindir=Path('/opt/homebrew/opt/postgresql@17/bin')
data=root/'.dev/bulk-commit-native-pg'; log=root/'.dev/bulk-commit-native-postgres.log'
assert not data.exists()
def run(*args): return subprocess.run([str(bindir/args[0]),*args[1:]],check=True,text=True,capture_output=True)
started=False
try:
 run('initdb','-D',str(data),'-U','postgres','--auth=trust')
 with (data/'postgresql.conf').open('a') as f:f.write("\nwal_init_zero = off\nport = 55432\nlisten_addresses = '127.0.0.1'\n")
 run('pg_ctl','-D',str(data),'-l',str(log),'-o','-p 55432 -h 127.0.0.1 -k /tmp','start');started=True
 run('psql','-h','127.0.0.1','-p','55432','-U','postgres','-v','ON_ERROR_STOP=1','-c',"CREATE EXTENSION pg_broker VERSION '0.1.17'")
 settings=run('psql','-h','127.0.0.1','-p','55432','-U','postgres','-Atc',"SELECT current_setting('wal_init_zero'),current_setting('fsync'),current_setting('full_page_writes'),current_setting('synchronous_commit'),extversion FROM pg_extension WHERE extname='pg_broker'").stdout.strip()
 assert settings=='off|on|on|on|0.1.17',settings
 env=dict(os.environ,DATABASE_URL='host=127.0.0.1 port=55432 user=postgres dbname=postgres',PG_CONFIG=str(bindir/'pg_config'),PG_BROKER_REQUIRE_LZ4='1',PG_BROKER_TEST_PGDATA=str(data),PG_BROKER_TEST_PG_CTL=str(bindir/'pg_ctl'))
 with (root/'.dev/bulk-commit-native-tests.log').open('w') as f:
  result=subprocess.run([str(root/'.venv/bin/pytest'),'-q','tests/test_bulk_commit.py','tests/test_wire.py','tests/test_broker.py'],env=env,stdout=f,stderr=subprocess.STDOUT,timeout=240)
 print((root/'.dev/bulk-commit-native-tests.log').read_text())
 assert result.returncode==0,result.returncode
 final=run('psql','-h','127.0.0.1','-p','55432','-U','postgres','-Atc',"SELECT current_setting('wal_init_zero'),current_setting('fsync'),current_setting('full_page_writes'),current_setting('synchronous_commit')").stdout.strip()
 assert final=='off|on|on|on',final
 (root/'.dev/bulk-commit-native-result.json').write_text(json.dumps(dict(exit_code=result.returncode,initial_settings=settings,final_settings=final),indent=2)+'\n')
finally:
 if started:run('pg_ctl','-D',str(data),'-m','fast','-w','stop')
 if data.exists():shutil.rmtree(data)
