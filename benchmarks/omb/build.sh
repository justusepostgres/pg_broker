#!/usr/bin/env bash
set -euo pipefail
root=$(cd "$(dirname "$0")/../.." && pwd)
checkout=${OMB_CHECKOUT:-"$root/.dev/openmessaging-benchmark"}
revision=$(cat "$root/benchmarks/omb/revision")
if [[ ! -d "$checkout/.git" ]]; then
  git clone https://github.com/openmessaging/benchmark.git "$checkout"
fi
[[ -z $(git -C "$checkout" status --porcelain --untracked-files=no) ]] || { echo 'OMB tracked files have modifications' >&2; exit 1; }
git -C "$checkout" checkout "$revision"
cd "$checkout"
mvn -B -pl benchmark-framework -am install -DskipTests \
  -Dspotless.check.skip=true -Dcheckstyle.skip=true -Dlicense.skip=true -Dspotbugs.skip=true
