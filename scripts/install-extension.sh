#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
PG_CONFIG=${PG_CONFIG:-pg_config}
scripts/package-extension.sh
suffix=so; [[ $(uname) != Darwin ]] || suffix=dylib
install -m 755 "target/debug/libpg_broker.$suffix" "$($PG_CONFIG --pkglibdir)/pg_broker.$suffix.new"
mv "$($PG_CONFIG --pkglibdir)/pg_broker.$suffix.new" "$($PG_CONFIG --pkglibdir)/pg_broker.$suffix"
install -m 644 target/extension/pg_broker/* "$($PG_CONFIG --sharedir)/extension/"
