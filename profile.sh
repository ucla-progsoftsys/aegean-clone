#!/usr/bin/env bash
# Usage:
#   bash ./profile.sh
#   bash ./profile.sh node4 8001
set -e

node="${1:-node1}"
port="${2:-8000}"
profile="results/aegean/${node}.cpu.pprof"

test -f "$profile" || {
  echo "missing $profile"
  echo "run: AEGEAN_PROFILE_NODE=$node ./run.sh"
  exit 1
}

exec go tool pprof -http=":$port" bin/aegean-node "$profile"
