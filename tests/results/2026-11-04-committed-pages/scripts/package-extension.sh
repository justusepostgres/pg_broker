#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p target/extension/pg_broker
cp crates/extension/pg_broker.control target/extension/pg_broker/
cp crates/extension/sql/pg_broker--0.1.0.sql target/extension/pg_broker/
cp crates/extension/sql/pg_broker--0.1.0--0.1.1.sql target/extension/pg_broker/
cat crates/extension/sql/pg_broker--0.1.0.sql crates/extension/sql/pg_broker--0.1.0--0.1.1.sql > target/extension/pg_broker/pg_broker--0.1.1.sql
cp crates/extension/sql/pg_broker--0.1.1--0.1.2.sql target/extension/pg_broker/
cat target/extension/pg_broker/pg_broker--0.1.1.sql crates/extension/sql/pg_broker--0.1.1--0.1.2.sql > target/extension/pg_broker/pg_broker--0.1.2.sql
cp crates/extension/sql/pg_broker--0.1.2--0.1.3.sql target/extension/pg_broker/
cat target/extension/pg_broker/pg_broker--0.1.2.sql crates/extension/sql/pg_broker--0.1.2--0.1.3.sql > target/extension/pg_broker/pg_broker--0.1.3.sql
cp crates/extension/sql/pg_broker--0.1.3--0.1.4.sql target/extension/pg_broker/
cat target/extension/pg_broker/pg_broker--0.1.3.sql crates/extension/sql/pg_broker--0.1.3--0.1.4.sql > target/extension/pg_broker/pg_broker--0.1.4.sql
cp crates/extension/sql/pg_broker--0.1.4--0.1.5.sql target/extension/pg_broker/
cat target/extension/pg_broker/pg_broker--0.1.4.sql crates/extension/sql/pg_broker--0.1.4--0.1.5.sql > target/extension/pg_broker/pg_broker--0.1.5.sql
cp crates/extension/sql/pg_broker--0.1.5--0.1.6.sql target/extension/pg_broker/
cat target/extension/pg_broker/pg_broker--0.1.5.sql crates/extension/sql/pg_broker--0.1.5--0.1.6.sql > target/extension/pg_broker/pg_broker--0.1.6.sql
cp crates/extension/sql/pg_broker--0.1.6--0.1.7.sql target/extension/pg_broker/
cat target/extension/pg_broker/pg_broker--0.1.6.sql crates/extension/sql/pg_broker--0.1.6--0.1.7.sql > target/extension/pg_broker/pg_broker--0.1.7.sql
cp crates/extension/sql/pg_broker--0.1.7--0.1.8.sql target/extension/pg_broker/
cat target/extension/pg_broker/pg_broker--0.1.7.sql crates/extension/sql/pg_broker--0.1.7--0.1.8.sql > target/extension/pg_broker/pg_broker--0.1.8.sql
cp crates/extension/sql/pg_broker--0.1.8--0.1.9.sql target/extension/pg_broker/
cat target/extension/pg_broker/pg_broker--0.1.8.sql crates/extension/sql/pg_broker--0.1.8--0.1.9.sql > target/extension/pg_broker/pg_broker--0.1.9.sql
cp crates/extension/sql/pg_broker--0.1.9--0.1.10.sql target/extension/pg_broker/
cat target/extension/pg_broker/pg_broker--0.1.9.sql crates/extension/sql/pg_broker--0.1.9--0.1.10.sql > target/extension/pg_broker/pg_broker--0.1.10.sql
cp crates/extension/sql/pg_broker--0.1.10--0.1.11.sql target/extension/pg_broker/
cat target/extension/pg_broker/pg_broker--0.1.10.sql crates/extension/sql/pg_broker--0.1.10--0.1.11.sql > target/extension/pg_broker/pg_broker--0.1.11.sql
cp crates/extension/sql/pg_broker--0.1.11--0.1.12.sql target/extension/pg_broker/
cat target/extension/pg_broker/pg_broker--0.1.11.sql crates/extension/sql/pg_broker--0.1.11--0.1.12.sql > target/extension/pg_broker/pg_broker--0.1.12.sql
cp crates/extension/sql/pg_broker--0.1.12--0.1.13.sql target/extension/pg_broker/
cat target/extension/pg_broker/pg_broker--0.1.12.sql crates/extension/sql/pg_broker--0.1.12--0.1.13.sql > target/extension/pg_broker/pg_broker--0.1.13.sql
cp crates/extension/sql/pg_broker--0.1.13--0.1.14.sql target/extension/pg_broker/
cat target/extension/pg_broker/pg_broker--0.1.13.sql crates/extension/sql/pg_broker--0.1.13--0.1.14.sql > target/extension/pg_broker/pg_broker--0.1.14.sql
cp crates/extension/sql/pg_broker--0.1.14--0.1.15.sql target/extension/pg_broker/
cat target/extension/pg_broker/pg_broker--0.1.14.sql crates/extension/sql/pg_broker--0.1.14--0.1.15.sql > target/extension/pg_broker/pg_broker--0.1.15.sql
cp crates/extension/sql/pg_broker--0.1.15--0.1.16.sql target/extension/pg_broker/
cat target/extension/pg_broker/pg_broker--0.1.15.sql crates/extension/sql/pg_broker--0.1.15--0.1.16.sql > target/extension/pg_broker/pg_broker--0.1.16.sql
cp crates/extension/sql/pg_broker--0.1.16--0.1.17.sql target/extension/pg_broker/
cat target/extension/pg_broker/pg_broker--0.1.16.sql crates/extension/sql/pg_broker--0.1.16--0.1.17.sql > target/extension/pg_broker/pg_broker--0.1.17.sql
cp crates/extension/sql/pg_broker--0.1.17--0.1.18.sql target/extension/pg_broker/
cat target/extension/pg_broker/pg_broker--0.1.17.sql crates/extension/sql/pg_broker--0.1.17--0.1.18.sql > target/extension/pg_broker/pg_broker--0.1.18.sql
cp crates/extension/sql/pg_broker--0.1.18--0.1.19.sql target/extension/pg_broker/
cat target/extension/pg_broker/pg_broker--0.1.18.sql crates/extension/sql/pg_broker--0.1.18--0.1.19.sql > target/extension/pg_broker/pg_broker--0.1.19.sql
