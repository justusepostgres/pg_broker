#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p target/extension/pg_broker
cp crates/extension/pg_broker.control target/extension/pg_broker/
cp crates/extension/sql/pg_broker--0.1.0.sql target/extension/pg_broker/
cp crates/extension/sql/pg_broker--0.1.0--0.1.1.sql target/extension/pg_broker/
cat crates/extension/sql/pg_broker--0.1.0.sql crates/extension/sql/pg_broker--0.1.0--0.1.1.sql > target/extension/pg_broker/pg_broker--0.1.1.sql
