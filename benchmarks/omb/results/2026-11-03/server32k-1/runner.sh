#!/usr/bin/env bash
# Single-system diagnosis, not a Kafka comparison or acceptance trial.
set -euo pipefail
label=${1:?Usage: oci-server-profile.sh unique-label}
[[ "$label" =~ ^[a-z0-9][a-z0-9-]{0,31}$ ]] || exit 2
cd /home/ubuntu
repo=/home/ubuntu/pg-broker
name="omb-server-$label"
out="/home/ubuntu/results/$name"
image=sha256:6821d31236114005fd88bc092f63721b540538169b8f048e09771ed68e24da19
candidate=/home/ubuntu/candidate-trace017/pg-broker-gateway
mkdir "$out"
exec 9>/home/ubuntu/omb-pair.lock
flock -n 9
[[ -z $(sudo docker ps -q) ]]
! pgrep -f '^java .*io.openmessaging.benchmark.Benchmark ' >/dev/null
! pgrep -f '^java .* kafka.Kafka ' >/dev/null
[[ $(df --output=avail -BG /home/ubuntu | tail -1 | tr -dc 0-9) -gt 15 ]]
! sudo docker volume inspect "$name-data" >/dev/null 2>&1
printf '%s  %s\n' 9ac61469507464d8a6f3ba54c667bbad0bb8b2dc8c31ef94c23c845284082727 "$candidate" | sha256sum -c - >"$out/binaries.txt"
monitor_pid=''
cleanup() {
 if [[ -n "$monitor_pid" ]]; then kill "$monitor_pid" 2>/dev/null || true; wait "$monitor_pid" 2>/dev/null || true; fi
 sudo docker stop "$name-gateway" >/dev/null 2>&1 || true
 if [[ $(sudo docker inspect --format '{{.State.Running}}' "$name-db" 2>/dev/null) = true ]]; then
  sudo docker kill --signal=SIGINT "$name-db" >/dev/null
  sudo docker wait "$name-db" >/dev/null
 fi
}
trap cleanup EXIT
trap 'exit 143' TERM
trap 'exit 130' INT
cat >"$out/instrumentation.json" <<'JSON'
{"kind":"server-maintenance-slow-sql-client-gc","acceptance_run":false,"slow_statement_ms":10,"parameter_logging":false,"io_timing":true,"wal_io_timing":true,"maintenance_trace":true,"host_vmstat_seconds":1,"postgres_log_rotation":{"size":"16m","files":4}}
JSON
cp "$repo/benchmarks/omb/oci-server-profile.sh" "$out/runner.sh"
for file in server-profile.sql server-profile-stats.sql verify-storage.sql; do cp "$repo/benchmarks/omb/$file" "$out/"; done
for migration in 0.1.5--0.1.6 0.1.6--0.1.7; do cp "$repo/crates/extension/sql/pg_broker--$migration.sql" "$out/"; done
cp /home/ubuntu/trace017-source-hashes.json "$out/source-hashes.json"
cp /home/ubuntu/omb-volume-profile.json "$out/volume-profile.json"
sha256sum "$out"/*.sql >"$out/sql-checksums.txt"
uname -a >"$out/host.txt"
java -version 2>"$out/java.txt"
df -B1 /home/ubuntu >"$out/free-before.txt"
sudo docker image inspect "$image" --format '{{.Id}}' >"$out/image.txt"
sudo docker run -d --name "$name-db" --cpuset-cpus=0,1 --memory=8g --network host \
 --log-driver=json-file --log-opt=max-size=16m --log-opt=max-file=4 \
 --env-file broker-omb.env -v "$name-data:/data" "$image" container-db.sh
ready=false
for attempt in $(seq 1 60); do
 if sudo docker exec "$name-db" test -f /data/.broker-ready && sudo docker exec "$name-db" pg_isready -U postgres >/dev/null; then ready=true; break; fi
 sleep 1
done
[[ "$ready" = true ]]
for migration in 0.1.5--0.1.6 0.1.6--0.1.7; do sudo docker cp "$out/pg_broker--$migration.sql" "$name-db:/opt/pgsql/share/extension/"; done
sudo docker exec "$name-db" psql -U postgres -v ON_ERROR_STOP=1 \
 -c "ALTER EXTENSION pg_broker UPDATE TO '0.1.7'" \
 -c "ALTER TABLE broker.batches ALTER data SET COMPRESSION lz4"
sudo docker exec -i "$name-db" psql -U postgres <"$out/server-profile.sql"
sleep 1 # Allow the postmaster to process the configuration reload.
sudo docker exec "$name-db" psql -U postgres -Atc "SELECT name,setting FROM pg_settings WHERE name IN ('log_min_duration_statement','log_parameter_max_length','log_parameter_max_length_on_error','log_line_prefix','log_timezone','log_checkpoints','log_autovacuum_min_duration','track_io_timing','track_wal_io_timing') ORDER BY name" >"$out/settings.txt"
python3 - "$out/settings.txt" <<'PY'
import sys
settings = dict(line.rstrip('\n').split('|', 1) for line in open(sys.argv[1]))
assert settings == {'log_min_duration_statement':'10','log_parameter_max_length':'0','log_parameter_max_length_on_error':'0','log_line_prefix':'%m [%p] %a ','log_timezone':'UTC','log_checkpoints':'on','log_autovacuum_min_duration':'0','track_io_timing':'on','track_wal_io_timing':'on'}, settings
PY
sudo docker run -d --name "$name-gateway" --cpuset-cpus=0,1 --memory=1g --network host \
 --env-file broker-omb.env --env PG_BROKER_TRACE_RETENTION=1 \
 -v "$candidate:/usr/local/bin/pg-broker-gateway:ro" "$image"
sudo docker inspect "$name-db" "$name-gateway" --format '{{.Name}} {{.HostConfig.CpusetCpus}} {{.HostConfig.Memory}}' >"$out/broker-limits.txt"
sudo docker exec -i "$name-db" psql -U postgres -At <"$out/server-profile-stats.sql" >"$out/stats-before.json"
vmstat -t -w 1 420 >"$out/vmstat.txt" &
monitor_pid=$!
export HEAP_OPTS="-Xms1G -Xmx1G -Xlog:gc*=info,safepoint=info:file=$out/client-%t-%p.log:time,uptime,level,tags:filecount=4,filesize=16M"
printf '%s\n' "$HEAP_OPTS" >"$out/heap-options.txt"
cd "$repo"
OMB_CHECKOUT=/home/ubuntu/omb taskset -c 2,3 benchmarks/omb/run.sh 32k localhost:9092 "$out/broker.json"
sudo docker stop "$name-gateway" >/dev/null
sudo docker logs "$name-gateway" >"$out/gateway.log" 2>&1
sudo docker exec -i "$name-db" psql -U postgres -At <"$out/server-profile-stats.sql" >"$out/stats-after.json"
sudo docker kill --signal=SIGINT "$name-db" >/dev/null
sudo docker wait "$name-db" >/dev/null
sudo docker logs "$name-db" >"$out/postgres.log" 2>&1
sudo docker start "$name-db" >/dev/null
for attempt in $(seq 1 30); do
 if sudo docker exec "$name-db" pg_isready -U postgres >/dev/null; then break; fi
 sleep 1
done
sudo docker exec -i "$name-db" psql -U postgres -At <"$out/verify-storage.sql" >"$out/storage.txt"
sudo docker exec "$name-db" psql -U postgres -Atc "SELECT count(*) FILTER (WHERE pg_column_compression(data)='lz4'), bool_and(coalesce(pg_column_compression(data),'lz4')='lz4') FROM broker.batches" >"$out/compression.txt"
python3 - "$out" <<'PY'
import json,sys
from pathlib import Path
p=Path(sys.argv[1]); d=json.loads((p/'broker.json').read_text())
assert len(d['publishRate']) == 30
assert (p/'storage.txt').read_text().splitlines() == ['0.1.7|on|on|on|t|t','16|t']
n,valid=(p/'compression.txt').read_text().strip().split('|'); assert int(n)>0 and valid=='t'
assert 'retention_end' in (p/'gateway.log').read_text()
print('diagnostic max publish errors/s',max(d['publishErrorRate']))
PY
cleanup
trap - EXIT
df -B1 /home/ubuntu >"$out/free-after.txt"
date -u +%FT%TZ >"$out/complete.txt"
