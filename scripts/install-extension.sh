#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
PG_CONFIG=${PG_CONFIG:-pg_config}
CARGO_PROFILE=${CARGO_PROFILE:-debug}
case "$CARGO_PROFILE" in debug|release) ;; *) echo 'CARGO_PROFILE must be debug or release' >&2; exit 1 ;; esac
scripts/package-extension.sh
suffix=so; [[ $(uname) != Darwin ]] || suffix=dylib
install -m 755 "target/$CARGO_PROFILE/libpg_broker.$suffix" "$($PG_CONFIG --pkglibdir)/pg_broker.$suffix.new"
mv "$($PG_CONFIG --pkglibdir)/pg_broker.$suffix.new" "$($PG_CONFIG --pkglibdir)/pg_broker.$suffix"
install -m 644 target/extension/pg_broker/* "$($PG_CONFIG --sharedir)/extension/"
