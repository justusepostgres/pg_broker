#!/usr/bin/env bash
# Fresh Kafka 3.6.1 / SQL 0.1.7 pair on the dedicated four-core OCI host.
# No builds, profiling, or storage-policy changes during measurement.
set -euo pipefail
label=${1:?Usage: oci-pair.sh unique-label 10k|20k|32k|50k [kafka-first|broker-first]}
rate=${2:?offered-rate required}
order=${3:-kafka-first}
[[ "$label" =~ ^[a-z0-9][a-z0-9-]{0,39}$ ]] || exit 2
case "$rate" in 10k|20k|32k|50k) ;; *) exit 2 ;; esac
case "$order" in kafka-first|broker-first) ;; *) exit 2 ;; esac
cd /home/ubuntu
repo=/home/ubuntu/pg-broker
candidate=/home/ubuntu/candidate-pipeline015/pg-broker-gateway
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
[[ $(df --output=avail -BG /home/ubuntu | tail -1 | tr -dc 0-9) -gt $minimum_free ]]
[[ ! -e "/home/ubuntu/$name-kafka-data" ]]
! sudo docker volume inspect "$name-data" >/dev/null 2>&1
printf '%s  %s\n' 4c394f8a49e5a197600ef00a456e0f5d34d04939885a03d90d1fb1db33e4d8fc "$candidate" | sha256sum -c - >"$out/binaries.txt"
sudo docker image inspect "$image" --format '{{.Id}}' >"$out/image.txt"
sudo docker run --rm "$image" sha256sum /opt/pgsql/lib/pg_broker.so >"$out/extension-binary.txt"
printf '%s\n' "$order" >"$out/order.txt"
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
cp "$repo/benchmarks/omb/verify-storage.sql" "$out/verify-storage.sql"
for migration in 0.1.5--0.1.6 0.1.6--0.1.7; do
 cp "$repo/crates/extension/sql/pg_broker--$migration.sql" "$out/"
done
sha256sum "$out"/*.sql >"$out/sql-checksums.txt"
sed "s@/home/ubuntu/kafka-data-omb@/home/ubuntu/$name-kafka-data@" \
 "$repo/benchmarks/omb/kafka-rf1.properties" >"$out/kafka.properties"
kafka=/home/ubuntu/kafka-dist/kafka_2.13-3.6.1/bin
start_kafka() {
 local unit=$1
 sudo systemd-run --unit="$unit" --uid=ubuntu --property=MemoryMax=9G \
  --property=AllowedCPUs=0,1 --setenv=KAFKA_HEAP_OPTS='-Xms1G -Xmx1G' \
  "$kafka/kafka-server-start.sh" "$out/kafka.properties"
 for attempt in $(seq 1 60); do
  if timeout 1 bash -c '</dev/tcp/127.0.0.1/19092' 2>/dev/null; then return; fi
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
 OMB_CHECKOUT=/home/ubuntu/omb taskset -c 2,3 benchmarks/omb/run.sh "$rate" localhost:19092 "$out/kafka.json"
 # Read only the benchmark topic; coordinator-internal offsets can change as
 # members leave. Capture data-log endpoints before and after a clean restart.
 "$kafka/kafka-get-offsets.sh" --bootstrap-server localhost:19092 --topic 'test-topic-.*' --time -1 | sort >"$out/kafka-offsets-before.txt"
 sudo systemctl stop "$name-kafka"
 sudo journalctl -u "$name-kafka" --no-pager >"$out/kafka-server.log"
 start_kafka "$name-kafka-recovery"
 "$kafka/kafka-get-offsets.sh" --bootstrap-server localhost:19092 --topic 'test-topic-.*' --time -1 | sort >"$out/kafka-offsets-after.txt"
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
 sudo docker run -d --name "$name-gateway" --cpuset-cpus=0,1 --memory=1g --network host \
  --env-file broker-omb.env -v "$candidate:/usr/local/bin/pg-broker-gateway:ro" "$image"
 sudo docker inspect "$name-db" "$name-gateway" --format '{{.Name}} {{.HostConfig.CpusetCpus}} {{.HostConfig.Memory}}' >"$out/broker-limits.txt"
 cd "$repo"
 OMB_CHECKOUT=/home/ubuntu/omb taskset -c 2,3 benchmarks/omb/run.sh "$rate" localhost:9092 "$out/broker.json"
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
assert (p/'storage.txt').read_text().splitlines() == ['0.1.7|on|on|on|t|t', '16|t']
n, valid = (p/'compression.txt').read_text().strip().split('|')
assert int(n)>0 and valid=='t'
rows = [line.split(':') for line in (p/'kafka-offsets-after.txt').read_text().splitlines()]
assert len(rows)==16 and len({r[0] for r in rows})==1
assert {int(r[1]) for r in rows}==set(range(16)) and all(int(r[2])>0 for r in rows)
for system in ('kafka', 'broker'):
    d = json.loads((p/f'{system}.json').read_text())
    assert len(d['publishRate']) == (90 if sys.argv[2]=='50k' else 30), system
    # Preserve errors and warnings as results, never hide a failed load.
    print(system, 'max publish errors/s', max(d['publishErrorRate']),
          'client warnings/errors', len(re.findall(r'\b(?:WARN|WARNING|ERROR|Exception)\b',
          (p/f'{system}.json.log').read_text())))
PY
cleanup
trap - EXIT
df -B1 /home/ubuntu >"$out/free-after.txt"
date -u +%FT%TZ >"$out/complete.txt"
