#!/usr/bin/env bash
# Fresh Kafka 3.6.1 / SQL 0.1.7 pair on the dedicated four-core OCI host.
# No builds, profiling, or storage-policy changes during measurement.
set -euo pipefail
label=${1:?Usage: oci-pair.sh unique-label 10k|20k|32k|50k|75k [kafka-first|broker-first] [on|off]}
rate=${2:?offered-rate required}
order=${3:-kafka-first}
wal_init_zero=${4:-on}
external_client=${OMB_EXTERNAL_CLIENT:-0}
server_host=${OMB_SERVER_HOST:-127.0.0.1}
kafka_flush_policy=${OMB_KAFKA_FLUSH_POLICY:-per-append}
[[ "$label" =~ ^[a-z0-9][a-z0-9-]{0,39}$ ]] || exit 2
case "$rate" in 10k|20k|32k|50k|75k) ;; *) exit 2 ;; esac
case "$order" in kafka-first|broker-first) ;; *) exit 2 ;; esac
case "$wal_init_zero" in on|off) ;; *) exit 2 ;; esac
case "$external_client" in 0|1) ;; *) exit 2 ;; esac
case "$kafka_flush_policy" in per-append|background) ;; *) exit 2 ;; esac
if [[ "$external_client" = 1 ]]; then
 case "$rate" in 32k|50k|75k) ;; *) exit 2 ;; esac
fi
python3 - "$external_client" "$server_host" <<'PY'
import ipaddress, sys
address = ipaddress.IPv4Address(sys.argv[2])
if sys.argv[1] == '0':
    assert str(address) == '127.0.0.1'
else:
    assert any(address in ipaddress.ip_network(cidr) for cidr in
               ('10.0.0.0/8', '172.16.0.0/12', '192.168.0.0/16'))
PY
cd /home/ubuntu
repo=/home/ubuntu/pg-broker
candidate=${OMB_GATEWAY_PATH:-/home/ubuntu/candidate-pipeline015/pg-broker-gateway}
candidate_sha=${OMB_GATEWAY_SHA256:-4c394f8a49e5a197600ef00a456e0f5d34d04939885a03d90d1fb1db33e4d8fc}
[[ "$candidate" =~ ^/home/ubuntu/candidate-[a-z0-9-]+/pg-broker-gateway$ ]] || exit 2
[[ "$candidate_sha" =~ ^[a-f0-9]{64}$ ]] || exit 2
image=sha256:6821d31236114005fd88bc092f63721b540538169b8f048e09771ed68e24da19
name="omb-pair-$label"
out="/home/ubuntu/results/$name"
mkdir "$out" # Refuse reuse, including partially completed trials.
exec 9>/home/ubuntu/omb-pair.lock
flock -n 9
[[ -z $(sudo docker ps -q) ]]
! pgrep -f '^java .*io.openmessaging.benchmark.Benchmark ' >/dev/null
! pgrep -f ' kafka.Kafka ' >/dev/null
minimum_free=25
[[ "$rate" != 50k ]] || minimum_free=65
[[ "$rate" != 75k ]] || minimum_free=95
[[ $(df --output=avail -BG /home/ubuntu | tail -1 | tr -dc 0-9) -gt $minimum_free ]]
[[ ! -e "/home/ubuntu/$name-kafka-data" ]]
! sudo docker volume inspect "$name-data" >/dev/null 2>&1
printf '%s  %s\n' "$candidate_sha" "$candidate" >"$out/gateway-sha256.txt"
sha256sum -c "$out/gateway-sha256.txt" >"$out/binaries.txt"
sudo docker image inspect "$image" --format '{{.Id}}' >"$out/image.txt"
sudo docker run --rm "$image" sha256sum /opt/pgsql/lib/pg_broker.so >"$out/extension-binary.txt"
printf '%s\n' "$order" >"$out/order.txt"
printf '%s\n' "$wal_init_zero" >"$out/wal-init-zero.txt"
printf '%s\n' "$kafka_flush_policy" >"$out/kafka-flush-policy.txt"
python3 - "$out" "$external_client" "$server_host" <<'PY'
import json, sys
from pathlib import Path
Path(sys.argv[1], 'placement.json').write_text(json.dumps({
    'kind': 'separate-client' if sys.argv[2] == '1' else 'loopback',
    'server_host': sys.argv[3]}, indent=2) + '\n')
PY
cleanup() {
 sudo systemctl stop "$name-kafka" "$name-kafka-recovery" >/dev/null 2>&1 || true
 sudo docker stop "$name-gateway" >/dev/null 2>&1 || true
 if [[ $(sudo docker inspect --format '{{.State.Running}}' "$name-db" 2>/dev/null) = true ]]; then
  sudo docker kill --signal=SIGINT "$name-db" >/dev/null
  sudo docker wait "$name-db" >/dev/null
 fi
}
trap cleanup EXIT
trap 'exit 143' TERM
trap 'exit 130' INT
uname -a >"$out/host.txt"
java -version 2>"$out/java.txt"
lsblk -b -o NAME,SIZE,FSTYPE,MOUNTPOINTS >"$out/disks.txt"
cp /home/ubuntu/omb-volume-profile.json "$out/volume-profile.json"
df -B1 /home/ubuntu >"$out/free-before.txt"
cp "$repo/benchmarks/omb/oci-pair.sh" "$out/runner.sh"
cp "$repo/benchmarks/omb/measurement.py" "$out/measurement.py"
sha256sum "$out/measurement.py" >"$out/measurement-checksum.txt"
cp "$repo/benchmarks/omb/verify-storage.sql" "$out/verify-storage.sql"
for migration in 0.1.5--0.1.6 0.1.6--0.1.7; do
 cp "$repo/crates/extension/sql/pg_broker--$migration.sql" "$out/"
done
sha256sum "$out"/*.sql >"$out/sql-checksums.txt"
sed "s@/home/ubuntu/kafka-data-omb@/home/ubuntu/$name-kafka-data@" \
 "$repo/benchmarks/omb/kafka-rf1.properties" >"$out/kafka.properties"
sed -i "s@PLAINTEXT://127.0.0.1:19092@PLAINTEXT://$server_host:19092@g" "$out/kafka.properties"
if [[ "$kafka_flush_policy" = background ]]; then
 # Kafka 3.6.1's default value. PostgreSQL durability remains unchanged.
 sed -i 's/^log.flush.interval.messages=1$/log.flush.interval.messages=9223372036854775807/' "$out/kafka.properties"
fi
kafka=/home/ubuntu/kafka-dist/kafka_2.13-3.6.1/bin
run_load() {
 local system=$1 port=$2
 if [[ "$external_client" = 0 ]]; then
  OMB_CHECKOUT=/home/ubuntu/omb taskset -c 2,3 "$repo/benchmarks/omb/run.sh" "$rate" "$server_host:$port" "$out/$system.json"
  return
 fi
 # The local controller runs the workload on the second VM, then uploads a
 # completed evidence directory atomically. No SSH private key leaves the controller.
 python3 - "$out" "$system" "$label" "$rate" "$server_host:$port" <<'PY'
import json, sys
from pathlib import Path
p, system, label, rate, endpoint = sys.argv[1:]
Path(p, system + '-request.json').write_text(json.dumps({
    'label': label + '-' + system, 'rate': rate, 'bootstrap': endpoint,
    'system': system}, indent=2) + '\n')
PY
 local deadline=$((SECONDS + 1600))
 while [[ ! -f "$out/client-$system/complete.txt" ]]; do
  [[ $SECONDS -lt $deadline ]] || { echo "Client handoff timed out: $system" >&2; return 1; }
  sleep 2
 done
 python3 - "$out" "$system" "$rate" "$server_host:$port" "$repo" <<'PY'
import json, shutil, sys
from pathlib import Path
p, system, rate, endpoint, repo = sys.argv[1:]
p, repo = Path(p), Path(repo)
sys.path.insert(0, str(p))
from measurement import verify_measurement
source = p / ('client-' + system)
assert (source / 'bootstrap.txt').read_text().strip() == endpoint
assert (source / 'revision.txt').read_text() == (repo / 'benchmarks/omb/revision').read_text()
assert not (source / 'tracked-changes.txt').read_text().strip()
assert (source / 'result.json.workload.yaml').read_bytes() == (repo / f'benchmarks/omb/{rate}.yaml').read_bytes()
result = json.loads((source / 'result.json').read_text())
verify_measurement(result, (source / 'result.json.log').read_text(),
                   15 if rate in ('50k', '75k') else 5, require_timing=True)
for file in source.glob('result.json*'):
    assert file.is_file()
    shutil.copyfile(file, p / (system + file.name[len('result'):]))
PY
}
start_kafka() {
 local unit=$1
 sudo systemd-run --unit="$unit" --uid=ubuntu --property=MemoryMax=9G \
  --property=AllowedCPUs=0,1 --setenv=KAFKA_HEAP_OPTS='-Xms1G -Xmx1G' \
  "$kafka/kafka-server-start.sh" "$out/kafka.properties"
 for attempt in $(seq 1 60); do
  if timeout 1 bash -c "</dev/tcp/$server_host/19092" 2>/dev/null; then return; fi
  sleep 1
 done
 return 1
}
run_kafka() {
 local cluster_id
 cluster_id=$("$kafka/kafka-storage.sh" random-uuid)
 "$kafka/kafka-storage.sh" format -t "$cluster_id" -c "$out/kafka.properties"
 start_kafka "$name-kafka"
 sudo systemctl show "$name-kafka" -p MemoryMax -p AllowedCPUs >"$out/kafka-limits.txt"
 cd "$repo"
 run_load kafka 19092
 # Read only the benchmark topic; coordinator-internal offsets can change as
 # members leave. Capture data-log endpoints before and after a clean restart.
 "$kafka/kafka-get-offsets.sh" --bootstrap-server "$server_host:19092" --topic 'test-topic-.*' --time -1 | sort >"$out/kafka-offsets-before.txt"
 sudo systemctl stop "$name-kafka"
 sudo journalctl -u "$name-kafka" --no-pager >"$out/kafka-server.log"
 start_kafka "$name-kafka-recovery"
 "$kafka/kafka-get-offsets.sh" --bootstrap-server "$server_host:19092" --topic 'test-topic-.*' --time -1 | sort >"$out/kafka-offsets-after.txt"
 cmp "$out/kafka-offsets-before.txt" "$out/kafka-offsets-after.txt"
 sudo systemctl stop "$name-kafka-recovery"
 sudo journalctl -u "$name-kafka-recovery" --no-pager >"$out/kafka-recovery.log"
 cd /home/ubuntu
}
run_broker() {
 cd /home/ubuntu
 sudo docker run -d --name "$name-db" --cpuset-cpus=0,1 --memory=8g --network host \
  --env-file broker-omb.env -v "$name-data:/data" "$image" container-db.sh
 local ready=false
 for attempt in $(seq 1 60); do
  if sudo docker exec "$name-db" test -f /data/.broker-ready && \
   sudo docker exec "$name-db" psql -U postgres -Atc "SELECT extversion FROM pg_extension WHERE extname='pg_broker'" 2>/dev/null | grep -qx 0.1.5; then ready=true; break; fi
  sleep 1
 done
 [[ "$ready" = true ]]
 for migration in 0.1.5--0.1.6 0.1.6--0.1.7; do
  sudo docker cp "$out/pg_broker--$migration.sql" "$name-db:/opt/pgsql/share/extension/"
 done
 sudo docker exec "$name-db" psql -U postgres -v ON_ERROR_STOP=1 \
  -c "ALTER EXTENSION pg_broker UPDATE TO '0.1.7'" \
  -c "ALTER TABLE broker.batches ALTER data SET COMPRESSION lz4"
 sudo docker exec "$name-db" psql -U postgres -v ON_ERROR_STOP=1 \
  -c "ALTER SYSTEM SET wal_init_zero = '$wal_init_zero'" -c 'SELECT pg_reload_conf()'
 sleep 1
 sudo docker exec "$name-db" psql -U postgres -Atc "SELECT current_setting('wal_init_zero'),current_setting('wal_recycle'),current_setting('wal_segment_size'),current_setting('wal_sync_method'),current_setting('fsync'),current_setting('full_page_writes'),current_setting('synchronous_commit')" >"$out/wal-settings.txt"
 [[ $(cat "$out/wal-settings.txt") = "$wal_init_zero|on|16MB|fdatasync|on|on|on" ]]
 sudo docker run -d --name "$name-gateway" --cpuset-cpus=0,1 --memory=1g --network host \
  --env-file broker-omb.env --env "LISTEN_ADDR=$server_host:9092" --env "ADVERTISED_HOST=$server_host" \
  -v "$candidate:/usr/local/bin/pg-broker-gateway:ro" "$image"
 sudo docker inspect "$name-db" "$name-gateway" --format '{{.Name}} {{.HostConfig.CpusetCpus}} {{.HostConfig.Memory}}' >"$out/broker-limits.txt"
 cd "$repo"
 run_load broker 9092
 sudo docker stop "$name-gateway" >/dev/null
 sudo docker logs "$name-gateway" >"$out/gateway.log" 2>&1
 sudo docker kill --signal=SIGINT "$name-db" >/dev/null
 sudo docker wait "$name-db" >/dev/null
 sudo docker start "$name-db" >/dev/null
 for attempt in $(seq 1 30); do
  if sudo docker exec "$name-db" pg_isready -U postgres >/dev/null; then break; fi
  sleep 1
 done
 sudo docker exec -i "$name-db" psql -U postgres -At < "$out/verify-storage.sql" >"$out/storage.txt"
 sudo docker exec "$name-db" psql -U postgres -Atc "SELECT count(*) FILTER (WHERE pg_column_compression(data)='lz4'), bool_and(coalesce(pg_column_compression(data),'lz4')='lz4') FROM broker.batches" >"$out/compression.txt"
 cleanup
 cd /home/ubuntu
}
if [[ "$order" = kafka-first ]]; then systems=(kafka broker); else systems=(broker kafka); fi
for system in "${systems[@]}"; do
 "run_$system"
 [[ $(df --output=avail -BG /home/ubuntu | tail -1 | tr -dc 0-9) -gt 15 ]]
done
python3 - "$out" "$rate" <<'PY'
import json, re, sys
from pathlib import Path
p = Path(sys.argv[1])
sys.path.insert(0, str(p))
from measurement import verify_measurement
assert (p/'storage.txt').read_text().splitlines() == ['0.1.7|on|on|on|t|t', '16|t']
n, valid = (p/'compression.txt').read_text().strip().split('|')
assert int(n)>0 and valid=='t'
rows = [line.split(':') for line in (p/'kafka-offsets-after.txt').read_text().splitlines()]
assert len(rows)==16 and len({r[0] for r in rows})==1
assert {int(r[1]) for r in rows}==set(range(16)) and all(int(r[2])>0 for r in rows)
for system in ('kafka', 'broker'):
    d = json.loads((p/f'{system}.json').read_text())
    verify_measurement(d, (p/f'{system}.json.log').read_text(),
                       15 if sys.argv[2] in ('50k', '75k') else 5, require_timing=True)
    # Preserve errors and warnings as results, never hide a failed load.
    print(system, 'max publish errors/s', max(d['publishErrorRate']),
          'client warnings/errors', len(re.findall(r'\b(?:WARN|WARNING|ERROR|Exception)\b',
          (p/f'{system}.json.log').read_text())))
PY
cleanup
trap - EXIT
df -B1 /home/ubuntu >"$out/free-after.txt"
date -u +%FT%TZ >"$out/complete.txt"
